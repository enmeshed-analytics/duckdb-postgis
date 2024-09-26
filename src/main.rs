use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

mod file_load;

fn main() -> io::Result<()> {
    // Replace with the actual path to the file you're testing
    let path =
        Path::new("/Users/christophercarlon/Downloads/GLA_High_Street_boundaries_EPSG4326.gpkg");

    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    match file_load::determine_file_type(&buffer) {
        Ok(file_type) => {
            println!("File type determined: {:?}", file_type);
        }
        Err(e) => {
            println!("Failed to determine file type: {:?}", e);
        }
    }

    Ok(())
}
