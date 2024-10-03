use duckdb_transformer::duckdb_load::process_file;
use std::path::Path;

const TEST_FILES_DIR: &str = "test_files";

fn test_file_path(file_name: &str) -> String {
    Path::new(TEST_FILES_DIR)
        .join(file_name)
        .to_str()
        .unwrap()
        .to_string()
}

#[test]
fn test_process_geojson() {
    let file_path = test_file_path("hotosm_twn_populated_places_points_geojson.geojson");
    assert!(process_file(&file_path).is_ok());
}

#[test]
fn test_process_geopackage() {
    let file_path = test_file_path("GLA_High_Street_boundaries.gpkg");
    assert!(process_file(&file_path).is_ok());
}

// #[test]
// fn test_process_shapefile() {
//     let file_path = test_file_path("your_shapefile.shp");
//     assert!(process_file(&file_path).is_ok());
// }

// #[test]
// fn test_process_excel() {
//     let file_path = test_file_path("your_excel_file.xlsx");
//     assert!(process_file(&file_path).is_ok());
// }

// #[test]
// fn test_process_csv() {
//     let file_path = test_file_path("your_csv_file.csv");
//     assert!(process_file(&file_path).is_ok());
// }

// #[test]
// fn test_process_parquet() {
//     let file_path = test_file_path("your_parquet_file.parquet");
//     assert!(process_file(&file_path).is_ok());
// }

#[test]
fn test_process_invalid_file() {
    let file_path = test_file_path("invalid.txt");
    assert!(process_file(&file_path).is_err());
}
