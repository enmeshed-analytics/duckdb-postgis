// Example usage
mod duckdb_load;
use duckdb_load::core_processor::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file(
        "test_files/GLA_High_Street_boundaries.gpkg",
        "CHRIS_TABLE.xlsx",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "CHRIS",
    )?;
    Ok(())
}
