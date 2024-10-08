# Rust lib for GridWalk Backend

This Rust library uses DuckDB and serves as a data transformation layer in the Gridwalk architecture.

## Current v0.1.0 release notes

### This Rust library does the following things:

- Reads in a path for a geospatial data file (Geopackage, Shapefile, etc)
- Reads in a table name for the PostGIS database
- Loads this file into DuckDB
- Determines the schema and current CRS of the data - returns both
- Performs CRS transformation on the data if required - ensuring the CRS is ESPG:4326
- Loads the data into a PostGIS table with a correctly defined geometry column

### Improvements for release 0.1.1:

The plan is to have this take in both a file path and a UUID to be used as a table name. Changes will be made to ensure that the library improves its overall functionality:

- Account for when geometry columns have different names - such as "geom", "geometry", or something else!
- Handle cases where data may have several geometry columns, not just 1!
- Handle many different file formats - xlsx, csv, raster data, etc
- Discard rows where there may be errors in the geometry column / ensure the programme doesn't crash when a geometry error is encountered - skip over it and log it instead
