use duckdb::arrow::datatypes::Schema;
use duckdb::{Connection, Result};
use std::error::Error;
use std::fs::File;
use std::io::{self, Read};
use std::sync::Arc;

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

// Determine the file type that is being processed
fn determine_file_type(file_content: &[u8]) -> Result<FileType, Box<dyn Error>> {
    // Gather fiule data
    let header = &file_content[0..16.min(file_content.len())];

    // Check for file types
    if &header[0..4] == b"PK\x03\x04" {
        Ok(FileType::Excel)
    } else if &header[0..16] == b"SQLite format 3\0" {
        Ok(FileType::Geopackage)
    } else if &header[0..4] == b"\x00\x00\x27\x0A" {
        Ok(FileType::Shapefile)
    } else if &header[0..4] == b"PAR1" {
        Ok(FileType::Parquet)
    } else if header.starts_with(b"{") {
        let json_start = std::str::from_utf8(file_content)?;
        if json_start.contains("\"type\":")
            && (json_start.contains("\"FeatureCollection\"") || json_start.contains("\"Feature\""))
        {
            Ok(FileType::Geojson)
        } else {
            Err("Not a valid GeoJSON file".into())
        }
    } else {
        let file_text = std::str::from_utf8(file_content)?;
        let lines: Vec<&str> = file_text.lines().collect();
        if lines.len() >= 2
            && lines[0].split(',').count() > 1
            && lines[1].split(',').count() == lines[0].split(',').count()
            && file_text.is_ascii()
        {
            Ok(FileType::Csv)
        } else {
            Err("Unknown file type".into())
        }
    }
}

// Get the data schema and make available for Dynamo DB ingestion in the future with Arc
fn query_and_print_schema(conn: &Connection) -> Result<Arc<Schema>> {
    // Prep query
    let query = "SELECT * FROM data LIMIT 10";

    // Process query
    let mut stmt = conn.prepare(query)?;
    let arrow_result = stmt.query_arrow([])?;

    // Print the schema for logging
    let schema = arrow_result.get_schema();
    println!("Schema: {:?}", schema);

    Ok(schema)
}

// Load to postgis
fn load_data_postgis(conn: &Connection, table_name: &str) -> Result<()> {
    // Attach PostGIS database
    conn.execute(
        "ATTACH 'dbname=gridwalk user=admin password=password host=localhost port=5432' AS gridwalk_db (TYPE POSTGRES)",
        [],
    )?;

    // Let table name
    let my_table_name = table_name;

    // Drop Table
    let delete_if_table_exists_query = &format!(
        "
        DROP TABLE IF EXISTS gridwalk_db.{};
    ",
        my_table_name
    );

    conn.execute(delete_if_table_exists_query, [])?;

    // Create Table
    let create_table_query = &format!(
        "
        CREATE TABLE gridwalk_db.{} AS
        SELECT *
        FROM transformed_data;
    ",
        my_table_name
    );

    conn.execute(create_table_query, [])?;

    // Postgis Update Table
    let postgis_query = &format!(
        "CALL postgres_execute('gridwalk_db', '
        ALTER TABLE {} ADD COLUMN geom geometry;
        UPDATE {} SET geom = ST_GeomFromText(geom_wkt, 4326);
        ALTER TABLE {} DROP COLUMN geom_wkt;
        ');",
        table_name, table_name, table_name
    );

    conn.execute(&postgis_query, [])?;

    // Log if table creation in PostGIS is successful
    println!(
        "Table {} created and data inserted successfully",
        my_table_name
    );
    Ok(())
}

// Get the current CRS number to compare it to the 4326 target CRS
fn get_crs_number(conn: &Connection, file_path: &str) -> Result<String> {
    // Prep query
    let query = &format!(
        "SELECT layers[1].geometry_fields[1].crs.auth_code AS crs_number FROM st_read_meta('{}');",
        file_path
    );
    let mut stmt = conn.prepare(&query)?;

    // Run query
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let crs_number: String = row.get(0)?;
        Ok(crs_number)
    } else {
        panic!("CRS not found for the following file: {}", file_path)
    }
}

// Transform the CRS and create transformed_data table in duckdb for table for later use in PostGIS
fn transform_crs(conn: &Connection, file_path: &str, target_crs: &str) -> Result<String> {
    // Get the current CRS
    let current_crs = get_crs_number(conn, file_path)?;
    println!("Current CRS: {}", current_crs);

    // Check if the current CRS matches the target CRS
    if current_crs == target_crs {
        // Create the transformed_data table without transformation if current == target
        let create_table_query = "
            CREATE TABLE transformed_data AS
            SELECT
                *,
                ST_AsText(geometry) as geom_wkt
            FROM data;
        ";
        conn.execute(create_table_query, [])?;
    } else {
        // Create the transformed_data table with transformation if current =! target
        let create_table_query = format!(
            "CREATE TABLE transformed_data AS
             SELECT
                *,
                ST_AsText(ST_Transform(geometry, 'EPSG:{}', 'EPSG:{}', always_xy := true)) AS geom_wkt,
             FROM data;",
            current_crs, target_crs
        );
        conn.execute(&create_table_query, [])?;
    }

    // Drop the original geom column
    let drop_column_query = "ALTER TABLE transformed_data DROP COLUMN geometry;";
    conn.execute(drop_column_query, [])?;

    if current_crs == target_crs {
        Ok(format!(
            "CRS is already {}. Geometry converted to WKT and original geom column dropped.",
            target_crs
        ))
    } else {
        Ok(format!(
            "Transformation from EPSG:{} to EPSG:{} completed. Geometry converted to WKT and original geom column dropped.",
            current_crs, target_crs
        ))
    }
}

// Process file and call all functions
fn process_file(file_path: &str, file_type: &FileType) -> Result<()> {
    // Create connection that will be used throughout processing
    let conn = Connection::open_in_memory()?;

    // Ensure required extensions are installed and loaded
    conn.execute("INSTALL spatial;", [])?;
    conn.execute("LOAD spatial;", [])?;
    conn.execute("INSTALL postgres;", [])?;
    conn.execute("LOAD postgres;", [])?;

    // Prep table creation queries
    let create_table_query = match file_type {
        FileType::Geopackage | FileType::Shapefile | FileType::Geojson => {
            format!(
                "CREATE TABLE data AS
                 SELECT *
                 FROM ST_Read('{}');",
                file_path
            )
        }
        FileType::Excel => {
            format!(
                "CREATE TABLE data AS SELECT * FROM st_read('{}');",
                file_path
            )
        }
        FileType::Csv => {
            format!(
                "CREATE TABLE data AS SELECT * FROM read_csv('{}');",
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

    // Create 'data' table in DuckDB
    conn.execute(&create_table_query, [])?;

    // Fetch schema of loaded data
    query_and_print_schema(&conn)?;

    // Perform Geospatial transformation and create 'transformed_data' table for later use in PostGIS
    transform_crs(&conn, file_path, "4326")?;

    // Call to load data into postgres and handle the result
    load_data_postgis(&conn, "testing_123")?;
    Ok(())
}

// Launch process file function - this is what you'd call in main.rs for example
pub fn launch_process_file(file_path: &str) -> Result<(), io::Error> {
    // Open file
    let mut file = File::open(file_path)?;

    // Read file content into a bytes array
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    // Check the type of file
    let file_type = determine_file_type(&buffer).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Error determining file type: {}", e),
        )
    })?;

    // Print file type
    println!("Detected file type: {:?}", file_type);

    // Process the file
    match process_file(file_path, &file_type) {
        Ok(_) => {
            println!("Successfully loaded {:?}", file_type);
            Ok(())
        }
        Err(e) => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Error processing {:?}: {}", file_type, e),
        )),
    }
}
