# Rust Library for writing geospatial/non-geospatial data to Postgis using DuckDB

![Crates.io](https://img.shields.io/crates/d/duckdb-postgis)

```bash
cargo add duckdb-postgis
```

This now supports python and will soon be available on PyPi.

## Future Release v0.2.0 (Soon)

- Reads in geospatial data file types (Geopackage, Shapefile, GeoJSON, Parquet) and automatically detects the file format using magic numbers and content analysis.
- Reads in non-geospatial data (CSV, Excel) with automatic header detection and error handling.
- Automatically detects coordinate pairs in CSV/Excel files by looking for common naming patterns (e.g., longitude/latitude, x/y, easting/northing) and converts them to geometry.
- Performs CRS transformation on the data if required - ensures the CRS is ESPG:4326 (WGS84) for consistent spatial operations.
- Loads the data into a PostGIS table with a correctly defined geometry column, handling both single and multi-geometry columns.
- Provides a clean interface for both Rust and Python applications through Pyo3 bindings.
- Uses DuckDB as an intermediate processing engine.

## Future Improvements post-v0.2.0

- Allow users to specify a target CRS instead of defaulting to EPSG:4326
- Add support for more file formats and coordinate systems
- Improve error handling and logging
- Add support for batch processing multiple files

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

This can also be called in Python thanks to Pyo3.

```python
import duckdb_postgis

def test_duckdb_postgis_import():
    """Test function to verify the duckdb_postgis module is working correctly."""
    try:

        # Attempt to process the test file
        duckdb_postgis.process_file(
            "[add_file_path]",
            "[add_table]",
            "postgresql://admin:password@localhost:5432/[add_db_name]",
            "[add_schema]"
        )
        print("File processing completed successfully")
        return True

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False

if __name__ == "__main__":
    test_duckdb_postgis_import()
```
