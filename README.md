# DuckDB Rust lib for writing geospatial data to Postgis

![Crates.io](https://img.shields.io/crates/d/duckdb-postgis)

```bash
cargo add duckdb-postgis
```

## Current v0.1.8 release notes

### This Rust library does the following things

- Reads in a path for a geospatial data file (Geopackage, Shapefile, etc)
- Reads in a path for non geospatial data (xlsx, xsv, parquet, etc)
- Reads in a table name for the PostGIS database
- Loads this file into DuckDB
- Determines the schema and current CRS of the data - returns both
- Performs CRS transformation on the data if required - ensures the CRS is ESPG:4326
- Loads the data into a PostGIS table with a correctly defined geometry column

### Improvements for release 0.1.9

- Handle raster data file formats
- Discard rows where there may be errors in the geometry column / ensure the programme doesn't crash when a geometry error is encountered - skip over it and log it instead
- There are still bugs for loading parquet files and handling some types of geometry columns due to how they are named - these will be fixed in the next release

### Example usage

```rust
mod duckdb_load;
use duckdb_load::launch_process_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    launch_process_file(
        "test_files/hotosm_twn_populated_places_points_geojson.geojson",
        "test-table",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "test-schema",
    )?;
    Ok(())
}

```
