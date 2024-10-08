use duckdb::arrow::datatypes::Schema;
use duckdb::Connection;
use std::error::Error;
use std::fs::File;
use std::io::{self, Read};
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
// This will include a UUID in the future that will be used for the PostGIS table name
struct DuckDBFileProcessor {
    file_path: String,
    table_name: String,
    file_type: FileType,
    conn: Connection,
}

// Implementation for DuckDBFileProcessor
impl DuckDBFileProcessor {
    fn new_file(file_path: &str, table_name: &str) -> Result<Self, Box<dyn Error>> {
        // Determine FileType
        let file_type = Self::determine_file_type(file_path)?;

        // Create Connection Object
        let conn = Connection::open(":memory:")?;

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
        })
    }

    fn process_new_file(&self) -> Result<(), Box<dyn Error>> {
        // Call all the required methods
        self.create_data_table()?;
        self.query_and_print_schema()?;
        self.transform_crs("4326")?;
        self.load_data_postgis()?;
        Ok(())
    }

    fn determine_file_type(file_path: &str) -> Result<FileType, Box<dyn Error>> {
        // Open file and read into buffer
        let mut file = File::open(file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        // Read in header of file
        let header = &buffer[0..16.min(buffer.len())];

        // Check for FileType
        match header {
            b"PK\x03\x04" => Ok(FileType::Excel),
            b"SQLite format 3\0" => Ok(FileType::Geopackage),
            b"\x00\x00\x27\x0A" => Ok(FileType::Shapefile),
            b"PAR1" => Ok(FileType::Parquet),
            _ if header.starts_with(b"{") => {
                let json_start = std::str::from_utf8(&buffer)?;
                if json_start.contains("\"type\":")
                    && (json_start.contains("\"FeatureCollection\"")
                        || json_start.contains("\"Feature\""))
                {
                    Ok(FileType::Geojson)
                } else {
                    Err("Not a valid GeoJSON file".into())
                }
            }
            _ => {
                let file_text = std::str::from_utf8(&buffer)?;
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
    }

    fn create_data_table(&self) -> Result<(), Box<dyn Error>> {
        // Create initial 'data' table
        let query = match self.file_type {
            FileType::Geopackage | FileType::Shapefile | FileType::Geojson => {
                format!(
                    "CREATE TABLE data AS SELECT * FROM ST_Read('{}');",
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
                    "CREATE TABLE data AS SELECT * FROM parquet_scan('{}');",
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

    fn transform_crs(&self, target_crs: &str) -> Result<String, Box<dyn Error>> {
        // Get current CRS
        let current_crs = self.get_crs_number()?;
        println!("Current CRS: {}", current_crs);

        // Create two paths for 'match to target crs' and 'no match to target crs'
        let create_table_query = if current_crs == target_crs {
            "CREATE TABLE transformed_data AS SELECT *,
            ST_AsText(geom) as geom_wkt
            FROM data;"
        } else {
            &format!(
                "CREATE TABLE transformed_data AS SELECT *,
                ST_AsText(ST_Transform(geom, 'EPSG:{}', 'EPSG:{}', always_xy := true)) AS geom_wkt
                FROM data;",
                current_crs, target_crs
            )
        };

        // Excecute query and drop original geometry column
        self.conn.execute(create_table_query, [])?;
        self.conn
            .execute("ALTER TABLE transformed_data DROP COLUMN geom;", [])?;

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

    fn load_data_postgis(&self) -> Result<(), Box<dyn Error>> {
        // Attach Postgres DB instance
        self.conn.execute(
            "ATTACH 'dbname=gridwalk user=admin password=password host=localhost port=5432' AS gridwalk_db (TYPE POSTGRES)",
            [],
        )?;

        // Execute CRUD logic
        let delete_if_table_exists_query =
            &format!("DROP TABLE IF EXISTS gridwalk_db.{};", self.table_name);
        self.conn.execute(delete_if_table_exists_query, [])?;

        let create_table_query = &format!(
            "CREATE TABLE gridwalk_db.{} AS SELECT * FROM transformed_data;",
            self.table_name
        );
        self.conn.execute(create_table_query, [])?;

        let postgis_query = &format!(
            "CALL postgres_execute('gridwalk_db', '
            ALTER TABLE {} ADD COLUMN geom geometry;
            UPDATE {} SET geom = ST_GeomFromText(geom_wkt, 4326);
            ALTER TABLE {} DROP COLUMN geom_wkt;
            ');",
            self.table_name, self.table_name, self.table_name
        );
        self.conn.execute(postgis_query, [])?;

        println!(
            "Table {} created and data inserted successfully",
            self.table_name
        );
        Ok(())
    }
}

pub fn launch_process_file(file_path: &str, table_name: &str) -> Result<(), io::Error> {
    // Create new processor object
    let processor = DuckDBFileProcessor::new_file(file_path, table_name).map_err(|e| {
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
