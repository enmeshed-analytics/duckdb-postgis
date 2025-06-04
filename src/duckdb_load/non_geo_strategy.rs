use std::error::Error;

use crate::duckdb_load::core_processor::CoreProcessor;
use crate::duckdb_load::postgis_processor::PostgisProcessor;

/// Strategy for handling non-geometric data
/// This doesn't need an extra impl block like the geo strategy does. 
/// This is because it doesn't need to implement any extra transformation methods
pub struct NonGeoStrategy;

impl PostgisProcessor for NonGeoStrategy {
    fn process_data_into_postgis(&self, core_processor: &CoreProcessor) -> Result<(), Box<dyn Error>> {
        println!("LOADING NON GEOSPATIAL DATA");

        let schema_qualified_table = core_processor.get_schema_qualified_table();
        let create_table_query = &format!(
            "CREATE TABLE gridwalk_db.{} AS SELECT * FROM data;",
            schema_qualified_table
        );
        core_processor.conn().execute(create_table_query, [])?;

        println!(
            "Table {} created and data inserted successfully (no geometry columns)",
            core_processor.table_name()
        );
        Ok(())
    }
}