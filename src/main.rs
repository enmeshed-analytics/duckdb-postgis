// Example usage
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file(
        "/Users/cmcarlon/Downloads/designated-green-belt-land-borough.xls",
        "my_table",
    )?;
    Ok(())
}
