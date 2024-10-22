// Example usage
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file(
        "test_files/hotosm_twn_populated_places_points_geojson.geojson",
        "test-table",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "test-schema",
    )?;
    Ok(())
}
