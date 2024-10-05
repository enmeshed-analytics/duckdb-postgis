# Rust lib for GridWalk Backend

This DuckDB library serves as a data transformation layer in the Gridwalk architecture.

## Current v0.1.0 release notes

### This Rust library does the following things

  Reads in a path for a geospatial data file (Geopackage, Shapefile, etc)
  Loads this file into DuckDB
  Determines the schema and current CRS of the data
  Performs CRS transformation on the data if required - ensuring the CRS is ESPG:4326
  Loads the data into a PostGIS table with a correctly defined geometry column

### Future releases

  The plan is to have this take in both a file path and a UUID to be used as a table name
  Changes will be made to ensure that the library improved its overall functionality...
  Account for when geometry columns have different names - such as "geom" or "geometry"
  Handle cases where data may have several geometry columns
  Handle many different file formats - xlsx, csv, raster data, etc
  Discard rows where there may be errors in the geometry column / ensure the programme doesn't crash when a geometry error is encountered - skip over it and log it instead
  etc etc etc
