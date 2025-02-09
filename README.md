# DuckDB Rust lib for writing geospatial data to Postgis

![Crates.io](https://img.shields.io/crates/d/duckdb-postgis)

```bash
cargo add duckdb-postgis
```
This now supports python and will soon be available on PyPi.

## Current v0.1.9 release notes

### This Rust library does the following things

- Reads in a path for a geospatial data file (Geopackage, Shapefile, etc)
- Reads in a path for non geospatial data (xlsx, xsv, parquet, etc)
- Reads in a table name for the PostGIS database
- Loads this file into DuckDB
- Determines the schema and current CRS of the data - returns both
- Performs CRS transformation on the data if required - ensures the CRS is ESPG:4326
- Loads the data into a PostGIS table with a correctly defined geometry column

### Improvements for release 0.1.10

- Handle raster data file format?
- Discard rows where there may be errors in the geometry column / ensure the programme doesn't crash when a geometry error is encountered - skip over it and log it instead
- There are still bugs for loading parquet files and handling some types of geometry columns due to how they are named - these will be fixed in the next release
- Add flexibility for target CRS.

### Example usage

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
