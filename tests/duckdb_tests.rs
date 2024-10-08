// NEED TO REDO TESTS!!!
// use duckdb_transformer::duckdb_load::launch_process_file;

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::path::Path;

//     #[test]
//     fn test_process_geojson_file() {
//         let file_path = "test_files/hotosm_twn_populated_places_points_geojson.geojson";

//         // Ensure the file exists
//         assert!(Path::new(file_path).exists(), "Test file does not exist");

//         // Process the file
//         let result = launch_process_file(file_path);

//         // Check if the processing was successful
//         assert!(
//             result.is_ok(),
//             "Failed to process the file: {:?}",
//             result.err()
//         );
//     }
// }
