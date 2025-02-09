// Example usage
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file(
        "/Users/cmcarlon/Downloads/Road_LAeq_16h_London.zip",
        "test-table-1001",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "test-schema-2",
    )?;
    Ok(())
}
