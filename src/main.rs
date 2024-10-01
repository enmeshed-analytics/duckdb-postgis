mod duckdb_load;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Hardcoded file path for demonstration
    // In a real application, you might want to make this configurable
    let file_path = "tmp/GLA_High_Street_boundaries.gpkg";

    println!("Processing file: {}", file_path);

    // Call the process_file function from the duckload module
    match duckdb_load::process_file(file_path) {
        Ok(_) => println!("File processed successfully."),
        Err(e) => eprintln!("Error processing file: {}", e),
    }

    Ok(())
}
