mod duckdb_load;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "test_files/GLA_High_Street_boundaries.gpkg";
    println!("Processing file: {}", file_path);

    match duckdb_load::process_file(file_path) {
        Ok(_) => {
            println!("File processed successfully.");
            Ok(())
        }
        Err(e) => {
            eprintln!("Error processing file: {}", e);
            Err(Box::new(e))
        }
    }
}
