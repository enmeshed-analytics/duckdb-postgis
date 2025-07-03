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

// Add Display implementation for FileType
impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            FileType::Geopackage => "Geopackage",
            FileType::Shapefile => "Shapefile", 
            FileType::Geojson => "GeoJSON",
            FileType::Excel => "Excel",
            FileType::Csv => "CSV",
            FileType::Parquet => "Parquet",
        };
        write!(f, "{}", name)
    }
}

// Main processor struct that handles most common operations
pub struct CoreProcessor {
    file_path: String,
    table_name: String,
    file_type: FileType,
    conn: Connection,
    postgis_uri: String,
    schema_name: String,
    coordinate_columns: Option<(String, String)>,
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
        println!("Detected file type: {:?} for file: '{}'", file_type, file_path);

        let table_name = Self::clean_table_name(table_name);
        let conn = Connection::open_in_memory()?;
        
        // Load the spatial extension for DuckDB
        conn.execute("INSTALL spatial;", [])?;
        conn.execute("LOAD spatial;", [])?;
        conn.execute("INSTALL postgres;", [])?;
        conn.execute("LOAD postgres;", [])?;

        Ok(CoreProcessor {
            file_path: file_path.to_string(),
            table_name,
            file_type,
            conn,
            postgis_uri: postgis_uri.to_string(),
            schema_name: schema_name.to_string(),
            coordinate_columns: None,
        })
    }

    // Clean the table name so that the extension is removed
    fn clean_table_name(table_name: &str) -> String {
        table_name
            .rsplit_once('.')
            .map(|(name, _)| name)
            .unwrap_or(table_name)
            .trim()
            .to_string()
    }

    // This is the main launch method for the CoreProcessor
    fn launch_core_processor(&mut self) -> Result<(), Box<dyn Error>> {
        self.create_duckb_table()?;
        self.query_and_print_schema()?;
        let geom_columns = self.find_geometry_columns()?;

        self.attach_postgres_db()?;
        self.create_schema()?;
        
        let schema_qualified_table = self.get_schema_qualified_table();
        self.drop_existing_table(&schema_qualified_table)?;

        if geom_columns.is_empty() {
            let processor = NonGeoStrategy;
            processor.process_data_into_postgis(self)?;
        } else {
            let processor = GeoStrategy::new(geom_columns);
            processor.process_data_into_postgis(self)?;
        }

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
    fn find_geometry_columns(&mut self) -> Result<Vec<String>, Box<dyn Error>> {
        // For CSV/Excel files, look for coordinate pairs
        if matches!(self.file_type, FileType::Csv | FileType::Excel) {
            return self.find_coordinate_pairs();
        }
        
        // For geospatial formats
        let query = "
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'data'
        AND (
            data_type = 'GEOMETRY'
            OR (data_type = 'BLOB' AND (column_name LIKE '%geo%' OR column_name LIKE '%geom%'))
            OR (data_type != 'DOUBLE' AND data_type != 'INTEGER' AND data_type != 'VARCHAR' 
                AND (column_name LIKE '%geom%' OR column_name = 'geometry'))
        )";

        let mut stmt = self.conn.prepare(query)?;
        let mut rows = stmt.query([])?;
        let mut geom_columns = Vec::new();

        while let Some(row) = rows.next()? {
            let column_name: String = row.get(0)?;
            if column_name != "gdb_geomattr_data" {
                geom_columns.push(column_name);
            }
        }

        Ok(geom_columns)
    }

    // New method to find coordinate pairs and store them
    fn find_coordinate_pairs(&mut self) -> Result<Vec<String>, Box<dyn Error>> {
        // Get all column names with original case
        let query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'data'";
        let mut stmt = self.conn.prepare(query)?;
        let mut rows = stmt.query([])?;
        let mut columns = Vec::new();

        while let Some(row) = rows.next()? {
            let column_name: String = row.get(0)?;
            columns.push(column_name);
        }

        let mut coordinate_pairs = Vec::new();

        // Define coordinate pair patterns
        let coordinate_patterns = [
            ("X-coordinate", "Y-coordinate"),
            ("x-coordinate", "y-coordinate"),
            ("x_coordinate", "y_coordinate"),
            ("longitude", "latitude"),
            ("long", "lat"),
            ("lng", "lat"),
            ("lon", "lat"),
            ("easting", "northing"),
            ("east", "north"),
            ("point_x", "point_y"),
            ("pt_x", "pt_y"),
            ("x_coord", "y_coord"),
            ("xcoord", "ycoord"),
            // Could delete this last pattern - probably not needed
            ("x", "y"),
        ];

        // Look for matching patterns
        for (x_pattern, y_pattern) in coordinate_patterns.iter() {
            let x_col = columns.iter().find(|col| {
                let col_lower = col.to_lowercase();
                col_lower == *x_pattern || 
                col_lower.contains(&format!("{}", x_pattern)) &&
                (col_lower.contains("coord") || col_lower.contains("x"))
            });
            
            let y_col = columns.iter().find(|col| {
                let col_lower = col.to_lowercase();
                col_lower == *y_pattern || 
                col_lower.contains(&format!("{}", y_pattern)) &&
                (col_lower.contains("coord") || col_lower.contains("y"))
            });

            if let (Some(x_name), Some(y_name)) = (x_col, y_col) {
                println!("Found coordinate pair: {} (X) and {} (Y)", x_name, y_name);
                
                self.coordinate_columns = Some((x_name.clone(), y_name.clone()));
                
                let geom_name = format!("geom_from_{}_{}",
                    x_name.replace("-", "_").replace(" ", "_").replace("(", "").replace(")", ""),
                    y_name.replace("-", "_").replace(" ", "_").replace("(", "").replace(")", "")
                );
                
                coordinate_pairs.push(geom_name);
                break;
            }
        }

        if coordinate_pairs.is_empty() {
            println!("No coordinate pairs detected in CSV/Excel file");
        }

        Ok(coordinate_pairs)
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
    pub fn determine_file_type(file_path: &str) -> Result<FileType, Box<dyn Error>> {
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
        
        // Try content-based detection for GeoJSON
        if let Ok(file_type) = Self::detect_geojson(&buffer) {
            return Ok(file_type);
        }
        
        // Check file extension for CSV
        // TODO: This is a hack and we should use the content-based detection instead?? Maybe change this in the future
        let path = std::path::Path::new(file_path);
        if let Some(extension) = path.extension() {
            let ext = extension.to_string_lossy().to_lowercase();
            if ext == "csv" {
                println!("Detected CSV file by extension: {}", file_path);
                return Ok(FileType::Csv);
            }
        }

        Err("Unknown or unsupported file type".into())
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

                let shapefile_patterns: [&[u8]; 4] = [b".shp", b".dbf", b".prj", b".shx"];

                let is_excel = excel_patterns
                    .iter()
                    .any(|&pattern| rest.windows(pattern.len()).any(|window| window == pattern));

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

    fn detect_geojson(buffer: &[u8]) -> Result<FileType, Box<dyn Error>> {
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
        }

        Err("Unknown or unsupported file type".into())
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
                    "CREATE TABLE data AS SELECT * FROM read_xlsx('{}');",
                    self.file_path
                )
            }
            FileType::Csv => {
                format!(
                    "CREATE TABLE data AS SELECT * FROM read_csv('{}', ignore_errors=true, header=true);",
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

    pub fn get_coordinate_columns(&self) -> Option<&(String, String)> {
        self.coordinate_columns.as_ref()
    }
}

/// Public function to process a file
pub fn process_file(
    file_path: &str,
    table_name: &str,
    postgis_uri: &str,
    schema_name: &str,
) -> Result<(), io::Error> {
    let mut core_processor = CoreProcessor::create_core_processor(file_path, table_name, postgis_uri, schema_name)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Error creating processor for '{}': {}", file_path, e)))?;

    core_processor.launch_core_processor()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Error processing {} file '{}': {}", core_processor.file_type().to_string(), file_path, e)))?;

    Ok(())
}