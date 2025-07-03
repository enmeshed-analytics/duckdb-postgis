// Example usage
mod duckdb_load;
use duckdb_load::core_processor::process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    process_file(
        "/Users/cmcarlon/Downloads/OS_Open_Built_Up_Areas_GeoPackage/os_open_built_up_areas.gpkg",
        "os_open_built_up_areas",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "public",
    )?;
    Ok(())
}
