// Example usage
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file("test_files/GLA_High_Street_boundaries.gpkg", "my_table")?;
    Ok(())
}
