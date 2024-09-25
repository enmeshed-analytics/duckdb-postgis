use duckdb::{Connection, Result};
use std::path::Path;

pub fn transform_crs(
    conn: &Connection,
    file_path: &Path,
    target_crs: &str,
) -> Result<String, duckdb::Error> {
    // Load spatial extension
    conn.execute("INSTALL spatial;", [])?;
    conn.execute("LOAD spatial;", [])?;

    // Get the current CRS
    let current_crs = get_crs_number(conn, file_path)?;

    // Check if the crs is already OK
    if current_crs == target_crs {
        return Ok(format!("CRS is already {}", target_crs));
    }

    // Perform the transformation on the whole file
    let transform_query = format!(
        "CREATE TABLE transformed_data AS
         SELECT *, ST_Transform(geom, 'EPSG:{}', 'EPSG:{}', always_xy := true) AS transformed_geom
         FROM ST_Read('{}');",
        current_crs,
        target_crs,
        file_path.to_str().unwrap()
    );
    conn.execute(&transform_query, [])?;

    // Verify the transformation by checking a sample of the transformed geometry col
    let verify_query = "
        SELECT ST_AsText(transformed_geom) AS wkt_geom
        FROM transformed_data
        LIMIT 1;
    ";

    let mut stmt = conn.prepare(verify_query)?;
    let mut rows = stmt.query([])?;

    let mut transformation_verified = false;
    let mut result_message = String::new();

    if let Some(row) = rows.next()? {
        let wkt_geom: String = row.get(0)?;
        result_message = format!("Sample transformed geometry: {}", wkt_geom);
        transformation_verified = true;
    }

    // Clean up the temporary table
    conn.execute("DROP TABLE transformed_data;", [])?;

    if transformation_verified {
        Ok(format!(
            "Successfully transformed GeoPackage from EPSG:{} to EPSG:{}\n{}",
            current_crs, target_crs, result_message
        ))
    } else {
        Ok("Transformation failed or resulted in no valid geometries".to_string())
    }
}

pub fn get_crs_number(conn: &Connection, file_path: &Path) -> Result<String, duckdb::Error> {
    let query = format!(
        "SELECT layers[1].geometry_fields[1].crs.auth_code AS crs_number FROM st_read_meta('{}');",
        file_path.to_str().unwrap()
    );
    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let crs_number: String = row.get(0)?;
        Ok(crs_number)
    } else {
        Ok("CRS number not found".to_string())
    }
}

pub fn duckdb_transform(file_path: &str) -> Result<String, duckdb::Error> {
    let conn = Connection::open_in_memory()?;
    let geopackage_path = Path::new(file_path);
    transform_crs(&conn, geopackage_path, "4326")
}
