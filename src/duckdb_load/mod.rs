use duckdb::arrow::datatypes::Schema;
use duckdb::Connection;
use std::error::Error;
use std::fs::File;
use std::io::{self, Read, Seek};
use std::sync::Arc;

// Enum that represents potential FileTypes
// More will be added in the future
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
        // Call initial methods
        self.create_data_table()?;
        self.query_and_print_schema()?;
    
        // First, check if we have any geometry columns
        let query = "
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'data' 
        AND (data_type = 'GEOMETRY' OR 
            (data_type = 'BLOB' AND 
            (column_name LIKE '%geo%' OR column_name LIKE '%geom%')))";

        let mut stmt = self.conn.prepare(query)?;
        let mut rows = stmt.query([])?;
        
        // If we find any geometry columns
        if rows.next()?.is_some() {
            // Transform geometry columns and store the result
            let geom_columns = self.transform_geom_columns()?;
            // Pass the geometry columns to load_data_postgis
            self.load_data_postgis(&geom_columns)?;
        } else {
            // No geometry columns - do a simple table copy
            self.load_non_geo_data()?;
        }
    
        Ok(())
    }

    fn determine_file_type(file_path: &str) -> Result<FileType, Box<dyn Error>> {
        // Open file and read first 100 bytes for magic number detection
        let mut file = File::open(file_path)?;
        let mut header_buffer = [0u8; 100];
        let bytes_read = file.read(&mut header_buffer)?;
        let header = &header_buffer[..bytes_read];

        // First try magic number detection
        if let Some(file_type) = Self::match_magic_numbers(header) {
            return Ok(file_type);
        }

        // If magic numbers don't match, perform content-based detection
        let mut buffer = Vec::new();
        file.seek(std::io::SeekFrom::Start(0))?;
        file.read_to_end(&mut buffer)?;
        Self::detect_content_based_type(&buffer)
    }

    fn match_magic_numbers(header: &[u8]) -> Option<FileType> {
        match header {
            // Excel (XLSX) - PKZip signature
            [0x50, 0x4B, 0x03, 0x04, ..] => Some(FileType::Excel),
            
            // Excel (XLS) 
            [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1, ..] => Some(FileType::Excel),
            
            // Parquet
            [0x50, 0x41, 0x52, 0x31, ..] => Some(FileType::Parquet),
            
            // Geopackage (SQLite)
            [0x53, 0x51, 0x4C, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6F, 0x72, 0x6D, 0x61, 0x74, 0x20, 0x33, 0x00, ..] => {
                Some(FileType::Geopackage)
            }
            
            // Shapefile
            [0x00, 0x00, 0x27, 0x0A, ..] => Some(FileType::Shapefile),
            
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
                    || text_lower.contains("\"geometry\"")) {
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
        // Create initial data table
        let query = match self.file_type {
            FileType::Geopackage | FileType::Shapefile | FileType::Geojson => {
                format!(
                    "CREATE TABLE data AS SELECT * FROM st_read('{}');",
                    self.file_path
                )
            }
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
        println!("Schema: {:?}", schema);
        Ok(schema)
    }

    fn get_crs_number(&self) -> Result<String, Box<dyn Error>> {
        // Let and prep query
        let query = format!(
            "SELECT layers[1].geometry_fields[1].crs.auth_code AS crs_number
            FROM st_read_meta('{}');",
            self.file_path
        );
        let mut stmt = self.conn.prepare(&query)?;

        // Run query and return CRS number
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let crs_number: String = row.get(0)?;
            Ok(crs_number)
        } else {
            Err(format!("CRS not found for the following file: {}", self.file_path).into())
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
