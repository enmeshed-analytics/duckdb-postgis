use pyo3::prelude::*;

#[pyfunction]
fn process_file(
    file_path: &str,
    table_name: &str,
    postgis_uri: &str,
    schema_name: &str,
) -> PyResult<()> {
    crate::duckdb_load::core_processor::launch_process_file(file_path, table_name, postgis_uri, schema_name)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    Ok(())
}

#[pymodule]
#[pyo3(name = "duckdb_postgis")]
fn duckdb_postgis(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(process_file, m)?)?;
    Ok(())
}
