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
        // Call all the required methods
        self.create_data_table()?;
        self.query_and_print_schema()?;

        // Transform geometry columns and store the result
        let geom_columns = self.transform_geom_columns()?;

        // Pass the geometry columns to load_data_postgis
        self.load_data_postgis(&geom_columns)?;

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
            [0, 0, 39, 10, ..] => Ok(FileType::Shapefile),
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

    fn transform_geom_columns(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'data' AND data_type = 'GEOMETRY'";
        let mut stmt = self.conn.prepare(query)?;
        let mut rows = stmt.query([])?;
        let mut geom_columns = Vec::new();

        while let Some(row) = rows.next()? {
            let column_name: String = row.get(0)?;
            geom_columns.push(column_name);
        }

        println!("Geometry columns: {:?}", &geom_columns);

        // Call transform_crs for each geometry column
        let target_crs = "4326";
        for column in &geom_columns {
            self.transform_crs(column, target_crs)?;
        }

        Ok(geom_columns)
    }

    fn transform_crs(&self, geom_column: &str, target_crs: &str) -> Result<String, Box<dyn Error>> {
        let current_crs = self.get_crs_number()?;
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
