// Example usage
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file("test_files/2011 Greenbelt/GreenBelt2011.shp", "my_table")?;
    Ok(())
}
