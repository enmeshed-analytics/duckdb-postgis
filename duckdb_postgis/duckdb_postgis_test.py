# Testing that it works in Python
from duckdb_postgis import process_file
import duckdb_postgis

def test_duckdb_postgis_import():
    """Test function to verify the duckdb_postgis module is working correctly."""
    try:
        # Check that the module is working correctly and being called in Python
        print("THIS IS BEING CALLED IN PYTHON")
        print("Available functions:", dir(duckdb_postgis))
        # Attempt to process the test file
        process_file(
        "test_files/GLA_High_Street_boundaries.gpkg",
        "add_table",
        "postgresql://admin:password@localhost:5432/gridwalk",
        "add_schema",
        )
        print("File processing completed successfully")
        return True

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False

if __name__ == "__main__":
    test_duckdb_postgis_import()
