use std::error::Error;
use std::fs::File;
use std::io::Read;
use zip::ZipArchive;

use crate::duckdb_load::postgis_processor::PostgisProcessor;
use crate::duckdb_load::core_processor::FileType;
use crate::duckdb_load::core_processor::CoreProcessor;

pub struct GeoStrategy {
    geom_columns: Vec<String>,
}

impl GeoStrategy {
    pub fn new(geom_columns: Vec<String>) -> Self {
        Self { geom_columns }
    }
    
    /// Get the CRS number
    /// TODO: Need to add in other routes for other file types such as xlsx, csv, etc
    fn get_crs_number(&self, core_processor: &CoreProcessor) -> Result<String, Box<dyn Error>> {
        match core_processor.file_type() {
            FileType::Shapefile => {
                let file = File::open(core_processor.file_path())?;
                let mut archive = ZipArchive::new(file)?;
                let shapefile_path = CoreProcessor::find_shapefile_path(core_processor.file_path())?;
                let prj_path = shapefile_path.replace(".shp", ".prj");

                for i in 0..archive.len() {
                    let mut file = archive.by_index(i)?;
                    if file.name() == prj_path {
                        let mut prj_content = String::new();
                        file.read_to_string(&mut prj_content)?;

                        if prj_content.contains("OSGB") || prj_content.contains("27700") {
                            println!("Found British National Grid CRS in PRJ file");
                            return Ok("27700".to_string());
                        }
                    }
                }

                println!("No specific CRS found in PRJ file, defaulting to WGS84 (EPSG:4326)");
                Ok("4326".to_string())
            },
            FileType::Parquet => {
                self.infer_parquet_crs_from_data(core_processor)
            },
            FileType::Csv | FileType::Excel => {
                // TODO: maybe seperate out csv and excel to different routes
                // TODO: need to do same CRS logic for csv and excel as for parquet
                // DON'T JUST DEFAULT TO WGS84 - NEED TO INFER THE CRS FROM THE DATA
                println!("CSV/Excel file detected, defaulting to WGS84 (EPSG:4326)");
                Ok("4326".to_string())
            },
            _ => {
                let query = format!(
                    "SELECT layers[1].geometry_fields[1].crs.auth_code AS crs_number
                    FROM st_read_meta('{}');",
                    core_processor.file_path()
                );
                let mut stmt = core_processor.conn().prepare(&query)?;
                let mut rows = stmt.query([])?;

                if let Some(row) = rows.next()? {
                    let crs_number: String = row.get(0)?;
                    Ok(crs_number)
                } else {
                    Err(format!("CRS not found for the following file: {}", core_processor.file_path()).into())
                }
            }
        }
    }

    /// Infer CRS 
    fn infer_parquet_crs_from_data(&self, core_processor: &CoreProcessor) -> Result<String, Box<dyn Error>> {
        println!("Attempting to infer CRS from coordinate data...");
        
        for geom_column in &self.geom_columns {
            if let Ok(crs) = self.analyse_geometry_column(core_processor, geom_column) {
                return Ok(crs);
            }
        }
        
        // If coordinate analysis fails default to WGS84!!
        println!("Could not infer CRS from coordinate data, defaulting to EPSG:4326 (WGS84)");
        Ok("4326".to_string())
    }

    /// Analyse a specific geometry column to infer CRS
    fn analyse_geometry_column(&self, core_processor: &CoreProcessor, geom_column: &str) -> Result<String, Box<dyn Error>> {
        println!("Analyzing geometry column: {}", geom_column);
        
        let inspect_data_query = format!(
            "SELECT typeof({}), length({}), {} IS NOT NULL as has_data 
             FROM data 
             WHERE {} IS NOT NULL 
             LIMIT 5",
            geom_column, geom_column, geom_column, geom_column
        );
        
        println!("Inspecting column data...");
        let mut stmt = core_processor.conn().prepare(&inspect_data_query)?;
        let mut rows = stmt.query([])?;
        
        match rows.next()? {
            Some(row) => {
                let data_type: String = row.get(0)?;
                let data_length: i64 = row.get(1)?;
                let has_data: bool = row.get(2)?;
                println!("  Type: {}, Length: {}, Has data: {}", data_type, data_length, has_data);
                
                // Now we know there's data, try extraction methods...
                if let Ok(crs) = self.try_direct_wkb_extraction(core_processor, geom_column) {
                    return Ok(crs);
                }
                
                if let Ok(crs) = self.try_hex_wkb_extraction(core_processor, geom_column) {
                    return Ok(crs);
                }
                
                if let Ok(crs) = self.try_direct_text_extraction(core_processor, geom_column) {
                    return Ok(crs);
                }
                
                Err("No valid coordinates found in geometry column".into())
            }
            None => {
                // No rows returned - column has no non-null geometry data
                Err(format!("Geometry column '{}' contains no valid data", geom_column).into())
            }
        }
    }

    /// Try extracting coordinates assuming WKB format
    fn try_direct_wkb_extraction(&self, core_processor: &CoreProcessor, geom_column: &str) -> Result<String, Box<dyn Error>> {
        println!("Trying direct WKB extraction...");
        
        let query = format!(
            "SELECT 
                ST_X(ST_Centroid(ST_GeomFromWKB({}))) as x,
                ST_Y(ST_Centroid(ST_GeomFromWKB({}))) as y 
             FROM data 
             WHERE {} IS NOT NULL 
             LIMIT 10",
            geom_column, geom_column, geom_column
        );
        
        self.extract_coordinates_from_query(core_processor, &query)
    }

    /// Try extracting coordinates assuming HEX WKB format
    fn try_hex_wkb_extraction(&self, core_processor: &CoreProcessor, geom_column: &str) -> Result<String, Box<dyn Error>> {
        println!("Trying HEX WKB extraction...");
        
        let query = format!(
            "SELECT 
                ST_X(ST_Centroid(ST_GeomFromHEXWKB({}))) as x,
                ST_Y(ST_Centroid(ST_GeomFromHEXWKB({}))) as y 
             FROM data 
             WHERE {} IS NOT NULL 
             LIMIT 10",
            geom_column, geom_column, geom_column
        );
        
        self.extract_coordinates_from_query(core_processor, &query)
    }

    /// Try treating as text and converting
    fn try_direct_text_extraction(&self, core_processor: &CoreProcessor, geom_column: &str) -> Result<String, Box<dyn Error>> {
        println!("Trying direct text extraction...");
        
        let query = format!(
            "SELECT 
                ST_X(ST_Centroid(ST_GeomFromText(CAST({} AS VARCHAR)))) as x,
                ST_Y(ST_Centroid(ST_GeomFromText(CAST({} AS VARCHAR)))) as y 
             FROM data 
             WHERE {} IS NOT NULL 
             LIMIT 10",
            geom_column, geom_column, geom_column
        );
        
        self.extract_coordinates_from_query(core_processor, &query)
    }

    /// Common coordinate extraction logic
    fn extract_coordinates_from_query(&self, core_processor: &CoreProcessor, query: &str) -> Result<String, Box<dyn Error>> {
        let mut stmt = core_processor.conn().prepare(query)?;
        let mut rows = stmt.query([])?;
        
        let mut x_values = Vec::new();
        let mut y_values = Vec::new();
        let mut error_count = 0;
        
        while let Some(row) = rows.next()? {
            match (row.get::<_, f64>(0), row.get::<_, f64>(1)) {
                (Ok(x), Ok(y)) if x.is_finite() && y.is_finite() => {
                    x_values.push(x);
                    y_values.push(y);
                    println!("  Found coordinate: ({:.6}, {:.6})", x, y);
                }
                _ => {
                    error_count += 1;
                    if error_count <= 3 { 
                        println!("  Invalid coordinate in row");
                    }
                }
            }
        }
        
        if x_values.is_empty() {
            return Err(format!("No valid coordinates extracted (errors: {})", error_count).into());
        }
        
        let x_min = x_values.iter().cloned().fold(f64::INFINITY, f64::min);
        let x_max = x_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let y_min = y_values.iter().cloned().fold(f64::INFINITY, f64::min);
        let y_max = y_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        
        println!("Coordinate ranges - X: [{:.6}, {:.6}], Y: [{:.6}, {:.6}]", x_min, x_max, y_min, y_max);
        
        let inferred_crs = self.infer_crs_from_ranges(x_min, x_max, y_min, y_max)?;
        println!("Inferred CRS: EPSG:{}", inferred_crs);
        
        Ok(inferred_crs)
    }

    // logic to guess the CRS from the coordinate ranges
    fn infer_crs_from_ranges(&self, x_min: f64, x_max: f64, y_min: f64, y_max: f64) -> Result<String, Box<dyn Error>> {
        // WGS84 (EPSG:4326)
        if x_min >= -180.0 && x_max <= 180.0 && y_min >= -90.0 && y_max <= 90.0 {
            if (x_max - x_min) < 10.0 && (y_max - y_min) < 10.0 {
                return Ok("4326".to_string());
            }
        }
        
        // British National Grid (EPSG:27700)
        if x_min >= 0.0 && x_max <= 800000.0 && y_min >= 0.0 && y_max <= 1400000.0 {
            if x_min > 1000.0 && y_min > 1000.0 {
                return Ok("27700".to_string());
            }
        }
        
        // Web Mercator (EPSG:3857)
        if x_min >= -20037508.0 && x_max <= 20037508.0 && y_min >= -20037508.0 && y_max <= 20037508.0 {
            if (x_max - x_min) > 10000.0 || (y_max - y_min) > 10000.0 {
                return Ok("3857".to_string());
            }
        }
        
        // Default fallback
        println!("Could not determine CRS from coordinate ranges, defaulting to WGS84");
        Ok("4326".to_string())
    }

    /// Transform the geometry columns to the target CRS
    fn transform_geom_columns(&self, core_processor: &CoreProcessor) -> Result<(), Box<dyn Error>> {
        println!("Geometry columns: {:?}", self.geom_columns);
        let target_crs = "4326";
        
        let current_crs = self.get_crs_number(core_processor)?;
        println!("Current CRS for all columns: {}", current_crs);
        
        match core_processor.file_type() {
            FileType::Csv | FileType::Excel => {
                // Handle CSV/Excel files with coordinate pairs
                // CURRENTLY ASSUMES THAT CSVS AND EXCEL ONLY EVER HAVE 1 GEOMETRY COLUMN
                self.transform_coordinate_pairs(core_processor, &current_crs, target_crs)
            }
            FileType::Geopackage | FileType::Shapefile | FileType::Geojson | FileType::Parquet => {
                // Process other geospatial file formats
                let mut cols_to_keep = Vec::new();
                
                cols_to_keep.push("* EXCLUDE (".to_string());
                let excluded_columns: Vec<String> = self.geom_columns.iter()
                    .map(|col| format!("\"{}\"", col))
                    .collect();
                cols_to_keep.push(excluded_columns.join(", "));
                cols_to_keep.push(")".to_string());
                
                for column in &self.geom_columns {
                    if current_crs == target_crs {
                        cols_to_keep.push(format!(
                            ", ST_AsText(ST_Force2D(\"{}\")) as \"{}_wkt\"",
                            column, column
                        ));
                    } else {
                        cols_to_keep.push(format!(
                            ", ST_AsText(ST_Force2D(ST_Transform(\"{}\", 'EPSG:{}', 'EPSG:{}', always_xy := true))) AS \"{}_wkt\"",
                            column, current_crs, target_crs, column
                        ));
                    }
                }
                
                let create_table_query = format!(
                    "CREATE TABLE transformed_data AS SELECT {} FROM data;",
                    cols_to_keep.join("")
                );
                
                println!("Creating transformed_data table...");
                core_processor.conn().execute(&create_table_query, [])?;
                
                if current_crs == target_crs {
                    println!("All geometry columns already in target CRS ({}). Converted to WKT.", target_crs);
                } else {
                    println!("Transformed all geometry columns from EPSG:{} to EPSG:{} and converted to WKT.", current_crs, target_crs);
                }
                
                Ok(())
            }
        }
    }

    /// Handle coordinate pairs for CSV/Excel files
    fn transform_coordinate_pairs(&self, core_processor: &CoreProcessor, current_crs: &str, target_crs: &str) -> Result<(), Box<dyn Error>> {
        let (x_col, y_col) = core_processor.get_coordinate_columns()
            .ok_or("No coordinate columns detected")?;
        
        // Get the geometry column name (should be the first and only one for CSV/Excel)
        let geom_column = self.geom_columns.first()
            .ok_or("No geometry column found for coordinate pair")?;
        
        let create_table_query = if current_crs == target_crs {
            format!(
                "CREATE TABLE transformed_data AS 
                 SELECT *, 
                        ST_AsText(ST_Force2D(ST_Point(\"{}\", \"{}\"))) as \"{}_wkt\" 
                 FROM data 
                 WHERE \"{}\" IS NOT NULL AND \"{}\" IS NOT NULL;",
                x_col, y_col, geom_column, x_col, y_col
            )
        } else {
            format!(
                "CREATE TABLE transformed_data AS 
                 SELECT *, 
                        ST_AsText(ST_Force2D(ST_Transform(ST_Point(\"{}\", \"{}\"), 'EPSG:{}', 'EPSG:{}', always_xy := true))) as \"{}_wkt\" 
                 FROM data 
                 WHERE \"{}\" IS NOT NULL AND \"{}\" IS NOT NULL;",
                x_col, y_col, current_crs, target_crs, geom_column, x_col, y_col
            )
        };
        
        println!("Creating transformed_data table with coordinate pairs...");
        core_processor.conn().execute(&create_table_query, [])?;
        
        println!("Created geometry from coordinate pairs: {} and {}", x_col, y_col);
        Ok(())
    }
}

impl PostgisProcessor for GeoStrategy {
    fn process_data_into_postgis(&self, core_processor: &CoreProcessor) -> Result<(), Box<dyn Error>> {
        println!("LOADING GEOSPATIAL DATA");
        
        self.transform_geom_columns(core_processor)?;
        let schema_qualified_table = core_processor.get_schema_qualified_table();
        
        let create_table_query = format!(
            "CREATE TABLE gridwalk_db.{} AS SELECT * FROM transformed_data;",
            schema_qualified_table
        );
        core_processor.conn().execute(&create_table_query, [])?;
        println!("Data copied to PostgreSQL table: {}", schema_qualified_table);
        
        let mut postgis_queries = Vec::new();
        
        for geom_column in &self.geom_columns {
            let target_crs = "4326";
            // Use exception handling to return NULL for invalid WKT data
            // Still need to filter these out of the final where clause!
            // Otherwise, we'll get an error when the frontend tries to display on the map
            let postgis_query = format!(
                "ALTER TABLE {} ADD COLUMN \"{}\" geometry;
                
                 CREATE OR REPLACE FUNCTION safe_geom_from_text(wkt_text TEXT, srid INTEGER)
                 RETURNS geometry AS $$
                 BEGIN
                     RETURN ST_GeomFromText(wkt_text, srid);
                 EXCEPTION
                     WHEN OTHERS THEN
                         RETURN NULL;
                 END;
                 $$ LANGUAGE plpgsql;
                 
                 UPDATE {} 
                 SET \"{}\" = safe_geom_from_text(\"{}_wkt\", {})
                 WHERE \"{}_wkt\" IS NOT NULL 
                   AND \"{}_wkt\" != '';
                 
                 DROP FUNCTION safe_geom_from_text(TEXT, INTEGER);
                 ALTER TABLE {} DROP COLUMN \"{}_wkt\";",
                schema_qualified_table,        // 1. ALTER TABLE
                geom_column,                   // 2. ADD COLUMN
                schema_qualified_table,        // 3. UPDATE table
                geom_column,                   // 4. SET column
                geom_column,                   // 5. safe_geom_from_text wkt column
                target_crs,                    // 6. safe_geom_from_text srid
                geom_column,                   // 7. WHERE wkt column (first check)
                geom_column,                   // 8. WHERE wkt column (second check)
                schema_qualified_table,        // 9. DROP ALTER TABLE
                geom_column,                   // 10. DROP COLUMN wkt column
            );
            
            postgis_queries.push(postgis_query);
        }
        
        let combined_query = format!(
            "BEGIN TRANSACTION;\n{}",
            postgis_queries.join("\n")
        );
        
        let postgres_execute_query = format!(
            "CALL postgres_execute('gridwalk_db', '{}');",
            combined_query.replace("'", "''")
        );
        
        println!("PostGIS Query: {}", postgres_execute_query);
        
        core_processor.conn().execute(&postgres_execute_query, [])?;
        
        println!("Table {} created and geometry columns transformed successfully", 
                 core_processor.table_name());
        
        Ok(())
    }
}