// Example usage
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file(
        "/Users/cmcarlon/Downloads/osopenusrn_202410_gpkg/osopenusrn_202410.gpkg",
        "my_table",
    )?;
    Ok(())
}
