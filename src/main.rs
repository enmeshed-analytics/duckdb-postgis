// Example usage
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file("FILE NAME HERE", "TABLE NAME HERE", "POSTGIS URI HERE")?;
    Ok(())
}
