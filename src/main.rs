// Example usage
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file(
        "/Users/cmcarlon/Downloads/2011 Greenbelt/GreenBelt2011.shp",
        "test-table-2",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "test-schema-2",
    )?;
    Ok(())
}
