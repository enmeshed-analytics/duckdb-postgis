# Rust lib for GridWalk Backend
![Crates.io](https://img.shields.io/crates/d/duckdb-postgis)

This Rust library uses DuckDB and serves as a data transformation layer in the Gridwalk architecture.

## Current v0.1.2 release notes

### This Rust library does the following things:

- Reads in a path for a geospatial data file (Geopackage, Shapefile, etc)
- Reads in a table name for the PostGIS database
- Loads this file into DuckDB
- Determines the schema and current CRS of the data - returns both
- Performs CRS transformation on the data if required - ensuring the CRS is ESPG:4326
- Loads the data into a PostGIS table with a correctly defined geometry column

### Improvements for release 0.1.3:

- Handle raster data file formats
- Discard rows where there may be errors in the geometry column / ensure the programme doesn't crash when a geometry error is encountered - skip over it and log it instead
