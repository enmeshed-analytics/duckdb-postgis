// Example usage
mod duckdb_load;
use duckdb_load::core_processor::process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    process_file(
        "/Users/cmcarlon/Downloads/osopenusrn_202505_gpkg/osopenusrn_202505.gpkg",
        "PARQUETTEST",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "POTTERPOTTER",
    )?;
    Ok(())
}
