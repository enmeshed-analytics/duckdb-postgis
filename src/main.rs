mod duckdb_load;
use duckdb_load::duckdb_transform;

fn main() {
    println!("Starting DuckDB GeoPackage loader...");

    let file_path = "/Users/christophercarlon/Downloads/GLA_High_Street_boundaries.gpkg";
    match duckdb_transform(file_path) {
        Ok(crs) => println!("{}", crs),
        Err(e) => eprintln!("An error occurred: {}", e),
    };

    println!("DuckDB GeoPackage loader finished.");
}
