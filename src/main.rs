mod duckdb_load;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "test_files/hotosm_twn_populated_places_points_geojson.geojson";
    println!("Processing file: {}", file_path);

    match duckdb_load::launch_process_file(file_path) {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Error processing file: {}", e);
            Err(Box::new(e))
        }
    }
}
