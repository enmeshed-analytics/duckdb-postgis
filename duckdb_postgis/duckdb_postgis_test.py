# Testing that it works in Python
import duckdb_postgis

def test_duckdb_postgis_import():
    """Test function to verify the duckdb_postgis module is working correctly."""
    try:
        print("Available functions:", dir(duckdb_postgis))

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
