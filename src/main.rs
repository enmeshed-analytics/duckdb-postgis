// Example usage
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file(
        "test_files/green.zip",
        "test-table-1000",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "test-schema-2",
    )?;
    Ok(())
}
