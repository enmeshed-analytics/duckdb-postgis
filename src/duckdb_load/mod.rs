use duckdb::{Connection, Result};
use std::error::Error;
use std::fs::File;
use std::io::{self, Read};

// Enum to hold file types to match on
#[derive(Debug, PartialEq)]
pub enum FileType {
    Geopackage,
    Shapefile,
    Geojson,
    Excel,
    Csv,
    Parquet,
}

// Determine the file type that is being processed
fn determine_file_type(file_content: &[u8]) -> io::Result<FileType> {
    let header = &file_content[0..16.min(file_content.len())];
    if &header[0..4] == b"PK\x03\x04" {
        Ok(FileType::Excel)
    } else if &header[0..16] == b"SQLite format 3\0" {
        Ok(FileType::Geopackage)
    } else if &header[0..4] == b"\x00\x00\x27\x0A" {
        Ok(FileType::Shapefile)
    } else if &header[0..4] == b"PAR1" {
        Ok(FileType::Parquet)
    } else if header.starts_with(b"{") {
        let json_start = std::str::from_utf8(file_content).unwrap_or("");
        if json_start.contains("\"type\":")
            && (json_start.contains("\"FeatureCollection\"") || json_start.contains("\"Feature\""))
        {
            Ok(FileType::Geojson)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Not a valid GeoJSON file",
            ))
        }
    } else {
        let file_text = std::str::from_utf8(file_content).unwrap_or("");
        let lines: Vec<&str> = file_text.lines().collect();
        if lines.len() >= 2
            && lines[0].split(',').count() > 1
            && lines[1].split(',').count() == lines[0].split(',').count()
            && file_text.is_ascii()
        {
            Ok(FileType::Csv)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown file type",
            ))
        }
    }
}

// Get data schema
fn query_and_print_schema(conn: &Connection) -> Result<()> {
    let query = "SELECT * FROM data LIMIT 50";
    let mut stmt = conn.prepare(query)?;
    let arrow_result = stmt.query_arrow([])?;
    // Get the schema
    let schema = arrow_result.get_schema();
    println!("Schema: {:?}", schema);
    Ok(())
}

// Load to postgis
fn load_data_postgis(conn: &Connection) -> Result<(), Box<dyn Error>> {
    // Attach PostGIS database
    conn.execute(
        "ATTACH 'dbname=gridwalk user=admin password=password host=localhost port=5432' AS gridwalk_db (TYPE POSTGRES)",
        [],
    )?;

    // Drop the existing table if it exists
    conn.execute("DROP TABLE IF EXISTS gridwalk_db.data_1", [])?;

    // Create the new table structure
    let create_table_query = "
        CREATE TABLE gridwalk_db.data_1 AS
        SELECT *,
               geom::geometry AS geometry
        FROM data;
    ";
    conn.execute(create_table_query, [])?;

    println!("Table 'data_1' created and data inserted successfully");
    Ok(())
}

// DuckDB file loader
fn load_file_duckdb(file_path: &str, file_type: &FileType) -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute("INSTALL spatial;", [])?;
    conn.execute("LOAD spatial;", [])?;
    conn.execute("INSTALL postgres;", [])?;
    conn.execute("LOAD postgres;", [])?;

    let create_table_query = match file_type {
        FileType::Geopackage | FileType::Shapefile | FileType::Geojson => {
            format!(
                "CREATE TABLE data AS SELECT * FROM ST_Read('{}');",
                file_path
            )
        }
        FileType::Excel => {
            format!(
                "CREATE TABLE data AS SELECT * FROM read_excel('{}');",
                file_path
            )
        }
        FileType::Csv => {
            format!(
                "CREATE TABLE data AS SELECT * FROM read_csv_auto('{}');",
                file_path
            )
        }
        FileType::Parquet => {
            format!(
                "CREATE TABLE data AS SELECT * FROM parquet_scan('{}');",
                file_path
            )
        }
    };

    // Create the table in DuckDB
    conn.execute(&create_table_query, [])?;

    // Call to query and print data schema
    query_and_print_schema(&conn)?;

    // Call to load data into postgres and handle the result
    match load_data_postgis(&conn) {
        Ok(_) => println!("Data successfully loaded into PostgreSQL"),
        Err(e) => eprintln!("Error loading data into PostgreSQL: {}", e),
    }

    Ok(())
}

// Process file
pub fn process_file(file_path: &str) -> io::Result<()> {
    let mut file = File::open(file_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    let file_type = determine_file_type(&buffer)?;
    println!("Detected file type: {:?}", file_type);

    load_file_duckdb(file_path, &file_type).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Error loading {:?} into DuckDB: {}", file_type, e),
        )
    })?;

    println!("Successfully loaded {:?} into DuckDB", file_type);
    Ok(())
}
