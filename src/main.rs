// Example usage
mod duckdb_load;
use duckdb_load::core_processor::process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    process_file(
        "[insert file path here]",
        "[insert table name here]",
        "[insert postgres uri here]",
        "[insert schema name here]",
    )?;
    Ok(())
}
