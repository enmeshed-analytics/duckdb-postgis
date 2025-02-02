use duckdb::arrow::datatypes::Schema;
use duckdb::Connection;
use std::error::Error;
use std::fs::File;
use std::io::{self, Read, Seek};
use std::sync::Arc;
use zip::ZipArchive;

// Enum that represents potential FileTypes
#[derive(Debug, PartialEq)]
enum FileType {
    Geopackage,
    Shapefile,
    Geojson,
    Excel,
    Csv,
    Parquet,
}

// Struct representing core components
struct DuckDBFileProcessor {
    file_path: String,
    table_name: String,
    file_type: FileType,
    conn: Connection,
    postgis_uri: String,
    schema_name: String,
}

// Implementation for DuckDBFileProcessor
impl DuckDBFileProcessor {
    fn new_file(
        file_path: &str,
        table_name: &str,
        postgis_uri: &str,
        schema_name: &str,
    ) -> Result<Self, Box<dyn Error>> {
        // Determine FileType
        let file_type = Self::determine_file_type(file_path)?;

        // Create Connection Object
        let conn = Connection::open(":memory:")?;

        // Define postgis_uri
        let postgis_uri = postgis_uri;

        // Install and load required extensions
        conn.execute("INSTALL spatial;", [])?;
        conn.execute("LOAD spatial;", [])?;
        conn.execute("INSTALL postgres;", [])?;
        conn.execute("LOAD postgres;", [])?;

        Ok(Self {
            file_path: file_path.to_string(),
            table_name: table_name.to_string(),
            file_type,
            conn,
            postgis_uri: postgis_uri.to_string(),
            schema_name: schema_name.to_string(),
        })
    }

    fn process_new_file(&self) -> Result<(), Box<dyn Error>> {
        // Call initial methods to get both the table created and the schema
        self.create_data_table()?;
        self.query_and_print_schema()?;

        // First, check if we have any geometry columns
        let query = "
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'data'
        AND (
            data_type = 'GEOMETRY'
            OR (data_type = 'BLOB' AND (column_name LIKE '%geo%' OR column_name LIKE '%geom%'))
            OR column_name LIKE '%geom%'
            OR column_name LIKE '%geometry%'
        )";

        let mut stmt = self.conn.prepare(query)?;
        let mut rows = stmt.query([])?;

        // If we find any geometry columns
        if rows.next()?.is_some() {
            // Transform geometry columns and store the result
            let geom_columns = self.transform_geom_columns()?;
            // Pass the geometry columns to load_data_postgis
            self.load_data_postgis(&geom_columns)?;
        } else {
            // No geometry columns - read non geospatial data
            self.load_non_geo_data()?;
        }

        Ok(())
    }

    fn find_shapefile_path(zip_path: &str) -> Result<String, Box<dyn Error>> {
        // Open zip file
        let file = File::open(zip_path)?;
        let mut archive = ZipArchive::new(file)?;

        // Find first .shp file in the archive
        for i in 0..archive.len() {
            let file = archive.by_index(i)?;
            let name = file.name();
            if name.ends_with(".shp") {
                return Ok(name.to_string());
            }
        }

        Err("No .shp file found in ZIP archive".into())
    }

    fn determine_file_type(file_path: &str) -> Result<FileType, Box<dyn Error>> {
        // Open file and read first 150 bytes for magic number detection
        let mut file = File::open(file_path)?;
        let mut header_buffer = [0u8; 150];
        let bytes_read = file.read(&mut header_buffer)?;
        let header = &header_buffer[..bytes_read];

        // First try magic number detection
        if let Some(file_type) = Self::match_magic_numbers(header) {
            return Ok(file_type);
        }

        // If magic numbers don't match, perform content-based detection for either csv or geojson/json
        let mut buffer = Vec::new();
        file.seek(std::io::SeekFrom::Start(0))?;
        file.read_to_end(&mut buffer)?;
        Self::detect_content_based_type(&buffer)
    }

    fn match_magic_numbers(buffer: &[u8]) -> Option<FileType> {
        match buffer {
            // PKZip signature [0x50, 0x4B, 0x03, 0x04] detected
            [0x50, 0x4B, 0x03, 0x04, rest @ ..] => {
                // Define patterns for both file types - adjust sizes to match expected 13 elements
                let excel_patterns: [&[u8]; 13] = [
                    b"xl/worksheets",
                    b"xl/_rels",
                    b"docProps/",
                    b"[Content_Types]",
                    b"xl/workbook",
                    b"xl/styles",
                    b"xl/theme",
                    b"xl/strings",
                    b"xl/charts",
                    b"xl/drawings",
                    b"xl/sharedStrings",
                    b"xl/metadata",
                    b"xl/calc",
                ];

                // Adjust shapefile patterns to match expected 4 elements
                let shapefile_patterns: [&[u8]; 4] = [b".shp", b".dbf", b".prj", b".shx"];

                // Check for Excel patterns first
                let is_excel = excel_patterns
                    .iter()
                    .any(|&pattern| rest.windows(pattern.len()).any(|window| window == pattern));

                // Check for Shapefile patterns
                let is_shapefile = shapefile_patterns
                    .iter()
                    .any(|&pattern| rest.windows(pattern.len()).any(|window| window == pattern));

                match (is_excel, is_shapefile) {
                    (true, false) => Some(FileType::Excel),
                    (false, true) => Some(FileType::Shapefile),
                    (true, true) => {
                        // In case both patterns are found (unlikely) - return none
                        println!("Error: Both patterns found - check file - none returned");
                        None
                    }
                    (false, false) => None,
                }
            }
            // Excel (XLS) - Compound File Binary Format
            [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1, ..] => Some(FileType::Excel),
            // Parquet
            [0x50, 0x41, 0x52, 0x31, ..] => Some(FileType::Parquet),
            // Geopackage (SQLite)
            [0x53, 0x51, 0x4C, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6F, 0x72, 0x6D, 0x61, 0x74, 0x20, 0x33, 0x00, ..] => {
                Some(FileType::Geopackage)
            }
            _ => None,
        }
    }

    fn detect_content_based_type(buffer: &[u8]) -> Result<FileType, Box<dyn Error>> {
        // Try GeoJSON first
        if let Ok(text) = std::str::from_utf8(buffer) {
            let text_lower = text.trim_start().to_lowercase();

            if text_lower.starts_with("{")
                && text_lower.contains("\"type\"")
                && (text_lower.contains("\"featurecollection\"")
                    || text_lower.contains("\"feature\"")
                    || text_lower.contains("\"geometry\""))
            {
                return Ok(FileType::Geojson);
            }

            // Check for CSV last
            if Self::is_valid_csv(text) {
                return Ok(FileType::Csv);
            }
        }

        Err("Unknown or unsupported file type".into())
    }

    fn is_valid_csv(content: &str) -> bool {
        let lines: Vec<&str> = content.lines().take(5).collect();

        if lines.len() < 2 {
            return false;
        }

        let first_line_fields = lines[0].split(',').count();
        // Require at least 2 columns and check for consistency
        first_line_fields >= 2
            && lines[1..].iter().all(|line| {
                let fields = line.split(',').count();
                fields == first_line_fields
                    && line.chars().all(|c| c.is_ascii() || c.is_whitespace())
            })
    }

    fn create_data_table(&self) -> Result<(), Box<dyn Error>> {
        // Create initial data table - with different methods depending on the FileType
        let query = match self.file_type {
            FileType::Geopackage | FileType::Geojson => {
                format!(
                    "CREATE TABLE data AS SELECT * FROM st_read('{}');",
                    self.file_path
                )
            }
            FileType::Shapefile => {
                let shapefile_path = Self::find_shapefile_path(&self.file_path)?;
                println!("Shapefile Path Found: {}", shapefile_path);
                format!(
                    "CREATE TABLE data AS SELECT * FROM st_read('/vsizip/{}/{}');",
                    self.file_path, shapefile_path
                )
            }
            // TODO replace st_read method with new excel specific method in new release of duckdb - tbc
            FileType::Excel => {
                format!(
                    "CREATE TABLE data AS SELECT * FROM st_read('{}');",
                    self.file_path
                )
            }
            FileType::Csv => {
                format!(
                    "CREATE TABLE data AS SELECT * FROM read_csv('{}');",
                    self.file_path
                )
            }
            FileType::Parquet => {
                format!(
                    "CREATE TABLE data AS SELECT * FROM read_parquet('{}');",
                    self.file_path
                )
            }
        };
        self.conn.execute(&query, [])?;
        Ok(())
    }

    fn query_and_print_schema(&self) -> Result<Arc<Schema>, Box<dyn Error>> {
        // Create and prep query
        let query = "SELECT * FROM data LIMIT 10";
        let mut stmt = self.conn.prepare(query)?;

        // Run query
        let arrow_result = stmt.query_arrow([])?;
        let schema = arrow_result.get_schema();

        // Print and return schema
        println!("The data schema is: {:?}", schema);
        Ok(schema)
    }

    fn get_crs_number(&self) -> Result<String, Box<dyn Error>> {
        if self.file_type == FileType::Shapefile {
            // Read the .prj file from the zip
            let file = File::open(&self.file_path)?;
            let mut archive = ZipArchive::new(file)?;
            let shapefile_path = Self::find_shapefile_path(&self.file_path)?;
            let prj_path = shapefile_path.replace(".shp", ".prj");

            for i in 0..archive.len() {
                let mut file = archive.by_index(i)?;
                if file.name() == prj_path {
                    let mut prj_content = String::new();
                    file.read_to_string(&mut prj_content)?;

                    // Check for common British National Grid identifiers in the PRJ
                    if prj_content.contains("OSGB") || prj_content.contains("27700") {
                        println!("Found British National Grid CRS in PRJ file");
                        return Ok("27700".to_string());
                    }
                }
            }

            // If we couldn't determine from PRJ, assume British National Grid for data
            println!("No CRS found in PRJ file, assuming British National Grid (EPSG:27700)");
            Ok("27700".to_string())
        } else {
            // Original logic for non-shapefile types
            let query = format!(
                "SELECT layers[1].geometry_fields[1].crs.auth_code AS crs_number
                FROM st_read_meta('{}');",
                self.file_path
            );
            let mut stmt = self.conn.prepare(&query)?;
            let mut rows = stmt.query([])?;

            if let Some(row) = rows.next()? {
                let crs_number: String = row.get(0)?;
                Ok(crs_number)
            } else {
                Err(format!("CRS not found for the following file: {}", self.file_path).into())
            }
        }
    }

    fn transform_geom_columns(&self) -> Result<Vec<String>, Box<dyn Error>> {
        // Query to find both GEOMETRY and potential geometry BLOB columns
        let query = "
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'data'
            AND (data_type = 'GEOMETRY' OR
                (data_type = 'BLOB' AND column_name LIKE '%geo%' OR column_name LIKE '%geom%'))";

        let mut stmt = self.conn.prepare(query)?;
        let mut rows = stmt.query([])?;
        let mut geom_columns = Vec::new();

        while let Some(row) = rows.next()? {
            let column_name: String = row.get(0)?;
            let data_type: String = row.get(1)?;

            // Skip gdb_geomattr_data column - sometimes this column can be present
            // and it shouldn't be loaded as a geom column
            if column_name == "gdb_geomattr_data" {
                continue;
            }

            // Handle the column based on its type
            if data_type == "BLOB" {
                // Try to convert BLOB to geometry
                println!("BLOB FOUND");
            }
            geom_columns.push(column_name);
        }

        // Process geometry columns as before
        println!("Geometry columns: {:?}", &geom_columns);
        let target_crs = "4326";
        for column in &geom_columns {
            self.transform_crs(column, target_crs)?;
        }

        Ok(geom_columns)
    }

    fn transform_crs(&self, geom_column: &str, target_crs: &str) -> Result<String, Box<dyn Error>> {
        // Get current CRS
        let current_crs = self.get_crs_number()?;
        println!("Current CRS for column {}: {}", geom_column, current_crs);

        // Transform CRS if no match on target crs
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

        self.conn.execute(&create_table_query, [])?;
        self.conn.execute(
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

    fn load_data_postgis(&self, geom_columns: &[String]) -> Result<(), Box<dyn Error>> {
        // Attach Postgres DB instance
        println!("LOADING GEOSPATIAL DATA");
        self.conn.execute(
            &format!(
                "ATTACH '{}' AS gridwalk_db (TYPE POSTGRES)",
                self.postgis_uri
            ),
            [],
        )?;

        // Create schema if it doesn't exist - Execute this directly in PostgreSQL
        // Note: We need to escape single quotes in the SQL string
        let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\";", self.schema_name);
        self.conn.execute(
            &format!(
                "CALL postgres_execute('gridwalk_db', '{}');",
                create_schema_sql.replace('\'', "''")
            ),
            [],
        )?;

        // Schema qualified table name
        let schema_qualified_table = format!("\"{}\".\"{}\"", self.schema_name, self.table_name);

        // Execute CRUD logic - First drop the table if it exists
        let drop_table_sql = format!("DROP TABLE IF EXISTS {};", schema_qualified_table);
        self.conn.execute(
            &format!(
                "CALL postgres_execute('gridwalk_db', '{}');",
                drop_table_sql.replace('\'', "''")
            ),
            [],
        )?;

        // Create data in table
        let create_table_query = &format!(
            "CREATE TABLE gridwalk_db.{} AS SELECT * FROM transformed_data;",
            schema_qualified_table
        );
        self.conn.execute(create_table_query, [])?;

        // Construct PostGIS query for each geometry column
        let mut postgis_queries = Vec::new();
        for geom_column in geom_columns {
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
        self.conn.execute(postgis_query, [])?;

        println!(
            "Table {} created and data inserted successfully with geometry columns: {:?}",
            self.table_name, geom_columns
        );
        Ok(())
    }

    fn load_non_geo_data(&self) -> Result<(), Box<dyn Error>> {
        // Attach Postgres DB instance
        println!("LOADING NON GEOSPATIAL DATA");
        self.conn.execute(
            &format!(
                "ATTACH '{}' AS gridwalk_db (TYPE POSTGRES)",
                self.postgis_uri
            ),
            [],
        )?;

        // Create schema if it doesn't exist
        let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\";", self.schema_name);
        self.conn.execute(
            &format!(
                "CALL postgres_execute('gridwalk_db', '{}');",
                create_schema_sql.replace('\'', "''")
            ),
            [],
        )?;

        // Schema qualified table name
        let schema_qualified_table = format!("\"{}\".\"{}\"", self.schema_name, self.table_name);

        // Drop existing table if it exists
        let drop_table_sql = format!("DROP TABLE IF EXISTS {};", schema_qualified_table);
        self.conn.execute(
            &format!(
                "CALL postgres_execute('gridwalk_db', '{}');",
                drop_table_sql.replace('\'', "''")
            ),
            [],
        )?;

        // Create data in table directly from 'data' table (no transformation needed)
        let create_table_query = &format!(
            "CREATE TABLE gridwalk_db.{} AS SELECT * FROM data;",
            schema_qualified_table
        );
        self.conn.execute(create_table_query, [])?;

        println!(
            "Table {} created and data inserted successfully (no geometry columns)",
            self.table_name
        );
        Ok(())
    }
}

pub fn launch_process_file(
    file_path: &str,
    table_name: &str,
    postgis_uri: &str,
    schema_name: &str,
) -> Result<(), io::Error> {
    // Create new processor object
    let processor = DuckDBFileProcessor::new_file(file_path, table_name, postgis_uri, schema_name)
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Error creating FileProcessor for '{}': {}", file_path, e),
            )
        })?;

    println!(
        "Detected file type: {:?} for file: '{}'",
        processor.file_type, file_path
    );

    // Process the file
    processor.process_new_file().map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Error processing {:?} file '{}': {}",
                processor.file_type, file_path, e
            ),
        )
    })?;

    println!(
        "Successfully loaded {:?} file: '{}'",
        processor.file_type, file_path
    );
    Ok(())
}
