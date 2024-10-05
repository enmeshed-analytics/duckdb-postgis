// Example usage
use std::error::Error;

mod duckdb_load;

fn main() -> Result<(), Box<dyn Error>> {
    let file_path = "test_files/GLA_High_Street_boundaries.gpkg";
    println!("Processing file: {}", file_path);

    if let Err(e) = duckdb_load::launch_process_file(file_path) {
        eprintln!("Error processing file: {}", e);
        return Err(e.into());
    }

    println!("File processed successfully");
    Ok(())
}
