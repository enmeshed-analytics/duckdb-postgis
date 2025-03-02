use std::error::Error;
use std::fs::File;
use std::io::Read;
use zip::ZipArchive;

use crate::duckdb_load::postgis_processor::PostgisProcessor;
use crate::duckdb_load::core_processor::FileType;
use crate::duckdb_load::core_processor::CoreProcessor;

/// Strategy for handling geo data
pub struct GeoStrategy {
    geom_columns: Vec<String>,
}

impl GeoStrategy {
    pub fn new(geom_columns: Vec<String>) -> Self {
        Self { geom_columns }
    }
    
    /// Get the CRS number
    fn get_crs_number(&self, core_processor: &CoreProcessor) -> Result<String, Box<dyn Error>> {
        if core_processor.file_type() == FileType::Shapefile {
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

            println!("No CRS found in PRJ file, assuming British National Grid (EPSG:27700)");
            Ok("27700".to_string())
        } else {
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

    /// Transform the geometry columns to the target CRS
    fn transform_geom_columns(&self, core_processor: &CoreProcessor) -> Result<(), Box<dyn Error>> {
        println!("Geometry columns: {:?}", self.geom_columns);
        let target_crs = "4326";
        for column in &self.geom_columns {
            self.transform_crs(core_processor, column, target_crs)?;
        }
        Ok(())
    }

    /// Transform CRS for a specific column
    fn transform_crs(&self, core_processor: &CoreProcessor, geom_column: &str, target_crs: &str) -> Result<String, Box<dyn Error>> {
        let current_crs = self.get_crs_number(core_processor)?;
        println!("Current CRS for column {}: {}", geom_column, current_crs);

        let create_table_query = if current_crs == target_crs {
            format!(
                "CREATE TABLE transformed_data AS SELECT *,
                ST_AsText({}) as {}_wkt
                FROM data;",
                geom_column, geom_column
            )
        } else {
            format!(
                "CREATE TABLE transformed_data AS SELECT *,
                ST_AsText(ST_Transform({}, 'EPSG:{}', 'EPSG:{}', always_xy := true)) AS {}_wkt
                FROM data;",
                geom_column, current_crs, target_crs, geom_column
            )
        };

        core_processor.conn().execute(&create_table_query, [])?;
        core_processor.conn().execute(
            &format!("ALTER TABLE transformed_data DROP COLUMN {};", geom_column),
            [],
        )?;

        if current_crs == target_crs {
            Ok(format!(
                "CRS for column {} is already {}. Geometry converted to WKT and original geom column dropped.",
                geom_column, target_crs
            ))
        } else {
            Ok(format!(
                "Transformation of column {} from EPSG:{} to EPSG:{} completed. Geometry converted to WKT and original geom column dropped.",
                geom_column, current_crs, target_crs
            ))
        }
    }
}

impl PostgisProcessor for GeoStrategy {
    fn process_data_into_postgis(&self, core_processor: &CoreProcessor) -> Result<(), Box<dyn Error>> {
        println!("LOADING GEOSPATIAL DATA");
        
        // Transform geometry columns
        self.transform_geom_columns(core_processor)?;
        
        core_processor.attach_postgres_db()?;
        core_processor.create_schema()?;
        
        let schema_qualified_table = core_processor.get_schema_qualified_table();
        core_processor.drop_existing_table(&schema_qualified_table)?;

        // Create data in table with transformed data (geometry WKT)
        let create_table_query = &format!(
            "CREATE TABLE gridwalk_db.{} AS SELECT * FROM transformed_data;",
            schema_qualified_table
        );
        core_processor.conn().execute(create_table_query, [])?;

        // Process geometry columns with PostGIS
        let mut postgis_queries = Vec::new();
        for geom_column in &self.geom_columns {
            postgis_queries.push(format!(
                "ALTER TABLE {} ADD COLUMN {} geometry;
                UPDATE {} SET {} = ST_GeomFromText({}_wkt, 4326);
                ALTER TABLE {} DROP COLUMN {}_wkt;",
                schema_qualified_table,
                geom_column,
                schema_qualified_table,
                geom_column,
                geom_column,
                schema_qualified_table,
                geom_column
            ));
        }

        let postgis_query = &format!(
            "CALL postgres_execute('gridwalk_db', '{}');",
            postgis_queries.join("\n")
        );
        core_processor.conn().execute(postgis_query, [])?;

        println!(
            "Table {} created and data inserted successfully with geometry columns: {:?}",
            core_processor.table_name(), self.geom_columns
        );
        Ok(())
    }
}