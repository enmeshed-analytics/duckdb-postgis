use duckdb_postgis::duckdb_load::core_processor::{CoreProcessor, FileType};
use std::io::Write;
use tempfile::NamedTempFile;

#[cfg(test)]
mod file_type_tests {
    use super::*;

    #[test]
    fn test_geojson_detection() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, r#"{{
            "type": "FeatureCollection",
            "features": [{{
                "type": "Feature",
                "geometry": {{ "type": "Point", "coordinates": [-0.1, 51.5] }},
                "properties": {{ "name": "Test" }}
            }}]
        }}"#).unwrap();
        
        let file_path = temp_file.path().to_str().unwrap();
        let detected_type = CoreProcessor::determine_file_type(file_path).unwrap();
        
        assert_eq!(detected_type, FileType::Geojson);
    }

    #[test]
    fn test_csv_detection() {
        let mut temp_file = NamedTempFile::with_suffix(".csv").unwrap();
        writeln!(temp_file, "id,name,value").unwrap();
        writeln!(temp_file, "1,test,100").unwrap();
        
        let file_path = temp_file.path().to_str().unwrap();
        let detected_type = CoreProcessor::determine_file_type(file_path).unwrap();
        
        assert_eq!(detected_type, FileType::Csv);
    }

    #[test]
    fn test_geopackage_detection() {
        // Create a file with SQLite magic number (Geopackage uses SQLite format)
        let mut temp_file = NamedTempFile::with_suffix(".gpkg").unwrap();
        let sqlite_header = b"SQLite format 3\x00";
        temp_file.write_all(sqlite_header).unwrap();
        // Add some padding
        temp_file.write_all(&[0u8; 100]).unwrap();
        
        let file_path = temp_file.path().to_str().unwrap();
        let detected_type = CoreProcessor::determine_file_type(file_path).unwrap();
        
        assert_eq!(detected_type, FileType::Geopackage);
    }

    #[test]
    fn test_excel_xlsx_detection() {
        // Create a file with ZIP magic number + Excel content
        let mut temp_file = NamedTempFile::with_suffix(".xlsx").unwrap();
        // PKZip header
        temp_file.write_all(&[0x50, 0x4B, 0x03, 0x04]).unwrap();
        // Add Excel-specific content
        temp_file.write_all(b"xl/worksheets").unwrap();
        temp_file.write_all(&[0u8; 100]).unwrap();
        
        let file_path = temp_file.path().to_str().unwrap();
        let detected_type = CoreProcessor::determine_file_type(file_path).unwrap();
        
        assert_eq!(detected_type, FileType::Excel);
    }

    #[test]
    fn test_excel_xls_detection() {
        // Create a file with old Excel (XLS) magic number
        let mut temp_file = NamedTempFile::with_suffix(".xls").unwrap();
        // Compound File Binary Format header
        let xls_header = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
        temp_file.write_all(&xls_header).unwrap();
        temp_file.write_all(&[0u8; 100]).unwrap();
        
        let file_path = temp_file.path().to_str().unwrap();
        let detected_type = CoreProcessor::determine_file_type(file_path).unwrap();
        
        assert_eq!(detected_type, FileType::Excel);
    }

    #[test]
    fn test_shapefile_detection() {
        // Create a ZIP file with shapefile content
        let mut temp_file = NamedTempFile::with_suffix(".zip").unwrap();
        // PKZip header
        temp_file.write_all(&[0x50, 0x4B, 0x03, 0x04]).unwrap();
        // Add shapefile-specific content
        temp_file.write_all(b"test.shp").unwrap();
        temp_file.write_all(&[0u8; 50]).unwrap();
        temp_file.write_all(b"test.dbf").unwrap();
        temp_file.write_all(&[0u8; 100]).unwrap();
        
        let file_path = temp_file.path().to_str().unwrap();
        let detected_type = CoreProcessor::determine_file_type(file_path).unwrap();
        
        assert_eq!(detected_type, FileType::Shapefile);
    }

    #[test]
    fn test_parquet_detection() {
        // Create a file with Parquet magic number
        let mut temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        // Parquet magic number
        let parquet_header = [0x50, 0x41, 0x52, 0x31];
        temp_file.write_all(&parquet_header).unwrap();
        temp_file.write_all(&[0u8; 100]).unwrap();
        
        let file_path = temp_file.path().to_str().unwrap();
        let detected_type = CoreProcessor::determine_file_type(file_path).unwrap();
        
        assert_eq!(detected_type, FileType::Parquet);
    }

    #[test]
    fn test_file_type_display() {
        // Test the Display implementation for FileType
        assert_eq!(format!("{}", FileType::Geopackage), "Geopackage");
        assert_eq!(format!("{}", FileType::Shapefile), "Shapefile");
        assert_eq!(format!("{}", FileType::Geojson), "GeoJSON");
        assert_eq!(format!("{}", FileType::Excel), "Excel");
        assert_eq!(format!("{}", FileType::Csv), "CSV");
        assert_eq!(format!("{}", FileType::Parquet), "Parquet");
    }
}
