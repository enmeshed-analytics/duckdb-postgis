// Example usage
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file(
        "[add_file_path]",
        "[add_table]",
        "postgresql://admin:password@localhost:5432/[add_db_name]",
        "[add_schema]",
    )?;
    Ok(())
}
