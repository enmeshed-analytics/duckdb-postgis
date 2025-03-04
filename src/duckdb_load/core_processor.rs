use duckdb::arrow::datatypes::Schema;
use duckdb::Connection;
use std::error::Error;
use std::fs::File;
use std::io::{self, Read, Seek};
use std::sync::Arc;
use zip::ZipArchive;

use crate::duckdb_load::postgis_processor::PostgisProcessor;
use crate::duckdb_load::geo_strategy::GeoStrategy;
use crate::duckdb_load::non_geo_strategy::NonGeoStrategy;

// Enum that represents potential FileTypes
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum FileType {
    Geopackage,
    Shapefile,
    Geojson,
    Excel,
    Csv,
    Parquet,
}

// Main processor struct that handles most common operations
pub struct CoreProcessor {
    file_path: String,
    table_name: String,
    file_type: FileType,
    conn: Connection,
    postgis_uri: String,
    schema_name: String,
}

// Implementation for CoreProcessor
// The CorePrcessor contains common operations for all processors/strategies
// It also handles duckdb connections and extensions as well as schema creation
// It also handles file type detection and creation of the initial data table
// TODO: We could take out the common operations into a separate trait?
impl CoreProcessor {
    // Create new CoreProcessor
    fn create_core_processor(
        file_path: &str,
        table_name: &str,
        postgis_uri: &str,
        schema_name: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let file_type = Self::determine_file_type(file_path)?;
        let conn = Connection::open(":memory:")?;

        // Install and load required extensions
        conn.execute("INSTALL spatial;", [])?;
        conn.execute("LOAD spatial;", [])?;
        conn.execute("INSTALL postgres;", [])?;
        conn.execute("LOAD postgres;", [])?;

        Ok(Self {
            file_path: file_path.to_string(),
            table_name: Self::clean_table_name(table_name),
            file_type,
            conn,
            postgis_uri: postgis_uri.to_string(),
            schema_name: schema_name.to_string(),
        })
    }

    // Clean the table name so that the extension is removed
    fn clean_table_name(table_name: &str) -> String {
        // Remove file extension and any leading/trailing whitespace
        table_name
            .rsplit_once('.')
            .map(|(name, _)| name)
            .unwrap_or(table_name)
            .trim()
            .to_string()
    }

    // Process the new file
    // This is the main workflow for the CoreProcessor
    fn launch_core_processor(&self) -> Result<(), Box<dyn Error>> {
        // Setup duckdb table and print out the schema
        self.create_duckb_table()?;
        self.query_and_print_schema()?;
        
        // Determine if there are geometry columns
        let geom_columns = self.find_geometry_columns()?;
        
        // Create and execute the appropriate strategy
        // PostgisProcessor is the trait that both strategies implement
        // Depending on whether there are geometry columns, the appropriate strategy is applied!
        let strategy: Box<dyn PostgisProcessor> = if !geom_columns.is_empty() {
            println!("Geometry columns found");
            Box::new(GeoStrategy::new(geom_columns))
        } else {
            println!("No geometry columns found");
            Box::new(NonGeoStrategy)
        };
        
        // Execute the chosen strategy
        strategy.process_data_into_postgis(self)?;
        
        Ok(())
    }

    //TODO: Everything below here is common to all strategies and needs to be moved to a trait?
    // Attach the postgres database
    pub fn attach_postgres_db(&self) -> Result<(), Box<dyn Error>> {
        self.conn.execute(
            &format!(
                "ATTACH '{}' AS gridwalk_db (TYPE POSTGRES)",
                self.postgis_uri
            ),
            [],
        )?;
        Ok(())
    }

    // Create the schema
    pub fn create_schema(&self) -> Result<(), Box<dyn Error>> {
        let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\";", self.schema_name);
        self.conn.execute(
            &format!(
                "CALL postgres_execute('gridwalk_db', '{}');",
                create_schema_sql.replace('\'', "''")
            ),
            [],
        )?;
        Ok(())
    }

    // Get the schema qualified table
    pub fn get_schema_qualified_table(&self) -> String {
        format!("\"{}\".\"{}\"", self.schema_name, self.table_name)
    }

    // Drop the existing table
    pub fn drop_existing_table(&self, schema_qualified_table: &str) -> Result<(), Box<dyn Error>> {
        let drop_table_sql = format!("DROP TABLE IF EXISTS {};", schema_qualified_table);
        self.conn.execute(
            &format!(
                "CALL postgres_execute('gridwalk_db', '{}');",
                drop_table_sql.replace('\'', "''")
            ),
            [],
        )?;
        Ok(())
    }

    // Find the geometry columns
    fn find_geometry_columns(&self) -> Result<Vec<String>, Box<dyn Error>> {
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
        let mut geom_columns = Vec::new();

        while let Some(row) = rows.next()? {
            let column_name: String = row.get(0)?;
            println!("Geometry column name: {}", column_name);
            if column_name != "gdb_geomattr_data" {
                geom_columns.push(column_name);
            }
        }

        Ok(geom_columns)
    }

    // Find shapefile path if file is a zip
    pub fn find_shapefile_path(zip_path: &str) -> Result<String, Box<dyn Error>> {
        let file = File::open(zip_path)?;
        let mut archive = ZipArchive::new(file)?;

        for i in 0..archive.len() {
            let file = archive.by_index(i)?;
            let name = file.name();
            if name.ends_with(".shp") {
                return Ok(name.to_string());
            }
        }

        Err("No .shp file found in ZIP archive".into())
    }

    // Determine the file type based on the magic numbers
    fn determine_file_type(file_path: &str) -> Result<FileType, Box<dyn Error>> {
        let mut file = File::open(file_path)?;
        let mut header_buffer = [0u8; 150];
        let bytes_read = file.read(&mut header_buffer)?;
        let header = &header_buffer[..bytes_read];

        if let Some(file_type) = Self::match_magic_numbers(header) {
            return Ok(file_type);
        }

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

    // Create the data table in duckdb
    fn create_duckb_table(&self) -> Result<(), Box<dyn Error>> {
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

    // Query the data and print the schema
    fn query_and_print_schema(&self) -> Result<Arc<Schema>, Box<dyn Error>> {
        let query = "SELECT * FROM data LIMIT 10";
        let mut stmt = self.conn.prepare(query)?;
        let arrow_result = stmt.query_arrow([])?;
        let schema = arrow_result.get_schema();
        println!("The data schema is: {:?}", schema);
        Ok(schema)
    }

    // Getter methods for attributes that need to be accessed by strategies
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn file_type(&self) -> FileType {
        self.file_type
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }
}

/// Public function to process a file
pub fn process_file(
    file_path: &str,
    table_name: &str,
    postgis_uri: &str,
    schema_name: &str,
) -> Result<(), io::Error> {
    let processor = CoreProcessor::create_core_processor(file_path, table_name, postgis_uri, schema_name)
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Error creating processor for '{}': {}", file_path, e),
            )
        })?;

    println!(
        "Detected file type: {:?} for file: '{}'",
        processor.file_type, file_path
    );

    processor.launch_core_processor().map_err(|e| {
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