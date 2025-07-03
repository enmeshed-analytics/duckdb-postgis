# DuckDB-Postgis

A Rust Library for writing geospatial/non-geospatial data to Postgis using DuckDB.

![Crates.io](https://img.shields.io/crates/d/duckdb-postgis)

![83b1fb0d-1243-4a45-bb27-8d94a1f7bc39](https://github.com/user-attachments/assets/4c9610ca-719e-427e-82cb-03b9de802973)

---

## Current Release v0.2.5

```bash
cargo add duckdb-postgis
```

- Reads in geospatial data file types (Geopackage, Shapefile, GeoJSON, Parquet) and automatically detects the file format using magic numbers and content analysis.
- Reads in non-geospatial data (CSV, Excel) with automatic header detection and error handling.
- Automatically detects coordinate pairs in CSV/Excel files by looking for common naming patterns (e.g., longitude/latitude, x/y, easting/northing) and converts them to geometry.
- Performs CRS transformation on the data if required - ensures the CRS is ESPG:4326 (WGS84) for consistent spatial operations.
- Loads the data into a PostGIS table with a correctly defined geometry column, handling both single and multi-geometry columns.
- Provides a clean interface for Rust applications.
- Uses DuckDB as an intermediate processing engine.

## Future Improvements

- Allow users to specify a target CRS instead of defaulting to EPSG:4326
- Add support for more file formats and coordinate systems
- Improve error handling and logging
- Add support for batch processing multiple files
- Expand the range of file formats supported

## Example usage

```rust
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file(
        "[add_file_path]",
        "[add_table]",
        "postgresql://admin:password@localhost:5432/[add_db_name]",
        "[add_schema]"
    )?;
    Ok(())
}
```
