// Example usage
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file(
        "test_files/December HMO public register.xlsx",
        "test-table-hmo",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "test-schema-hmo",
    )?;
    Ok(())
}
