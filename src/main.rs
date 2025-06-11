// Example usage
mod duckdb_load;
use duckdb_load::core_processor::process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    process_file(
        "/Users/cmcarlon/Desktop/repdd/repd-q4-jan-2025.csv",
        "repd_q4_jan_2025",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "DESNZ",
    )?;
    Ok(())
}
