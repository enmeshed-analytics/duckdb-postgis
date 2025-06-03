// Example usage
mod duckdb_load;
use duckdb_load::core_processor::process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    process_file(
        "test_files/GLA_High_Street_boundaries.gpkg",
        "PARQUETTEST",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "CHRIS",
    )?;
    Ok(())
}
