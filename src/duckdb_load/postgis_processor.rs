use std::error::Error;
use crate::duckdb_load::core_processor::CoreProcessor;

/// Strategy trait for processing data
/// We could add different processors for different databases in the future!
/// TODO: Explore different databases and how to process data for each one
pub trait PostgisProcessor {
    /// Process data using the given processor
    fn process_data_into_postgis(&self, core_processor: &CoreProcessor) -> Result<(), Box<dyn Error>>;
}