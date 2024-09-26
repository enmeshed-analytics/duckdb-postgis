use std::io;

#[derive(Debug, PartialEq)]
pub enum FileType {
    Geopackage,
    Shapefile,
    Geojson,
    Excel,
    Csv,
    Parquet,
}

// Function to determine file type based on the first few bytes
pub fn determine_file_type(file_content: &[u8]) -> io::Result<FileType> {
    let header = &file_content[0..16.min(file_content.len())];

    if &header[0..4] == b"PK\x03\x04" {
        // Excel
        Ok(FileType::Excel)
    } else if &header[0..16] == b"SQLite format 3\0" {
        // Geopackage
        Ok(FileType::Geopackage)
    } else if &header[0..4] == b"\x00\x00\x27\x0A" {
        // Shapefile
        Ok(FileType::Shapefile)
    } else if &header[0..4] == b"PAR1" {
        // Parquet
        Ok(FileType::Parquet)
    } else if header.starts_with(b"{") {
        // Additional check for GeoJSON
        let json_start = std::str::from_utf8(file_content).unwrap_or("");
        if json_start.contains("\"type\":")
            && (json_start.contains("\"FeatureCollection\"") || json_start.contains("\"Feature\""))
        {
            Ok(FileType::Geojson)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Not a valid GeoJSON file",
            ))
        }
    } else {
        // Check for CSV by inspecting the first couple of lines
        let file_text = std::str::from_utf8(file_content).unwrap_or("");
        let lines: Vec<&str> = file_text.lines().collect();
        if lines.len() >= 2
            && lines[0].split(',').count() > 1
            && lines[1].split(',').count() == lines[0].split(',').count()
            && file_text.is_ascii()
        {
            Ok(FileType::Csv)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown file type",
            ))
        }
    }
}
