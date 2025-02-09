use pythonize::pythonize;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use sqlparser::parser::Parser;

mod opteryx_dialect;
pub use opteryx_dialect::OpteryxDialect;

/// Function to parse SQL statements from a string. Returns a list with
/// one item per query statement.
///
/// We always use 'opteryx' as the dialect for parsing, to help anyone
/// who is familiar with sqloxide to not assume the default behaviour
/// we have a _dialect parameter that is not used.
#[pyfunction]
#[pyo3(text_signature = "(sql, dialect)")]
fn parse_sql(py: Python, sql: String, _dialect: String) -> PyResult<PyObject> {
    let chosen_dialect = Box::new(OpteryxDialect {});
    let parse_result = Parser::parse_sql(&*chosen_dialect, &sql);

    let output = match parse_result {
        Ok(statements) => pythonize(py, &statements).map_err(|e| {
            let msg = e.to_string();
            PyValueError::new_err(format!("Python object serialization failed.\n\t{msg}"))
        })?,
        Err(e) => {
            let msg = e.to_string();
            return Err(PyValueError::new_err(format!(
                "Query parsing failed.\n\t{msg}"
            )));
        }
    };

    Ok(output.into())
}


#[pymodule]
fn compute(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_sql, m)?)?;
    Ok(())
}