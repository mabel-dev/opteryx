


use pyo3::prelude::*;

use pyo3::wrap_pyfunction;

mod sqloxide;
use sqloxide::{restore_ast, parse_sql};

mod list_ops;
use list_ops::{anyop_eq_numeric, anyop_eq_string};

#[pymodule]
fn compute(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_sql, m)?)?;
    m.add_function(wrap_pyfunction!(restore_ast, m)?)?;

    m.add_function(wrap_pyfunction!(anyop_eq_numeric, m)?)?;
    m.add_function(wrap_pyfunction!(anyop_eq_string, m)?)?;
    Ok(())
}