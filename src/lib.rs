


use pyo3::prelude::*;

use pyo3::wrap_pyfunction;

mod sqloxide;
use sqloxide::{restore_ast, parse_sql};


#[pymodule]
fn compute(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_sql, m)?)?;
    m.add_function(wrap_pyfunction!(restore_ast, m)?)?;

    Ok(())
}