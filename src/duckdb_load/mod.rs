use duckdb::{params, types::Value, Connection};
use std::error::Error;

pub fn duckdb_transform(file_content: &[u8]) -> Result<String, Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;

    // Load spatial extension
    conn.execute("INSTALL spatial;", [])?;
    conn.execute("LOAD spatial;", [])?;

    // Get the current CRS
    let current_crs = get_crs_number(&conn, file_content)?;

    // Check if the CRS is already OK
    if current_crs == "4326" {
        return Ok(format!("CRS is already 4326"));
    }

    // Perform the transformation using the file content
    let transform_query = "
        CREATE TABLE transformed_data AS
        SELECT *, ST_Transform(geom, ?, ?, always_xy := true) AS transformed_geom
        FROM ST_Read(?)
    ";

    // Use DuckDB blob to pass the byte array (file content)
    let blob_content = Value::Blob(file_content.to_vec());

    conn.execute(transform_query, params![current_crs, "4326", blob_content])?;

    // Verify the transformation by checking a sample of the transformed geometry
    let verify_query = "
        SELECT ST_AsText(transformed_geom) AS wkt_geom
        FROM transformed_data
        LIMIT 1;
    ";
    let mut stmt = conn.prepare(verify_query)?;
    let mut rows = stmt.query([])?;
    let mut result_message = String::new();

    if let Some(row) = rows.next()? {
        let wkt_geom: String = row.get(0)?;
        result_message = format!("Sample transformed geometry: {}", wkt_geom);
    }

    // Clean up the temporary table
    conn.execute("DROP TABLE transformed_data;", [])?;

    Ok(format!(
        "Successfully transformed dataset to EPSG:4326\n{}",
        result_message
    ))
}

pub fn get_crs_number(conn: &Connection, file_content: &[u8]) -> Result<String, Box<dyn Error>> {
    let query = "
        SELECT layers[1].geometry_fields[1].crs.auth_code AS crs_number
        FROM st_read_meta(?)
    ";

    // Use DuckDB blob to pass the byte array (file content)
    let blob_content = Value::Blob(file_content.to_vec());

    let mut stmt = conn.prepare(query)?;
    let mut rows = stmt.query(params![blob_content])?;

    if let Some(row) = rows.next()? {
        let crs_number: String = row.get(0)?;
        Ok(crs_number)
    } else {
        Ok("CRS number not found".to_string())
    }
}
