use pythonize::pythonize;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use sqlparser::parser::Parser;
// no PyDict needed when we accept a single Authorization string

mod opteryx_dialect;
mod temporal_parser;

pub use opteryx_dialect::OpteryxDialect;
pub use temporal_parser::{extract_temporal_for_clauses, TemporalExtractionResult, TemporalFilter};

/// Convert Python-style backreferences (\1, \2, etc.) to Rust-style ($1, $2, etc.)
fn convert_python_to_rust_backrefs(replacement: &str) -> String {
    let mut result = String::new();
    let mut chars = replacement.chars().peekable();
    
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(&next_ch) = chars.peek() {
                if next_ch.is_ascii_digit() {
                    // This is a backreference like \1
                    result.push('$');
                    // Don't consume the next char, just peek
                } else {
                    // Regular escape sequence, keep the backslash
                    result.push(ch);
                }
            } else {
                // Backslash at end of string
                result.push(ch);
            }
        } else {
            result.push(ch);
        }
    }
    
    result
}

/// Function to parse SQL statements from a string. Returns a list with
/// one item per query statement.
///
/// We always use 'opteryx' as the dialect for parsing, to help anyone
/// who is familiar with sqloxide to not assume the default behaviour
/// we have a _dialect parameter that is not used.
#[pyfunction]
#[pyo3(text_signature = "(sql, dialect)")]
fn parse_sql(py: Python, sql: String, _dialect: String) -> PyResult<Py<PyAny>> {
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

/// Extract temporal FOR clauses from SQL.
/// Returns a dictionary with 'clean_sql' (SQL with FOR clauses removed) 
/// and 'filters' (list of temporal filter information).
/// 
/// **Note**: This is a proof-of-concept. The Python implementation in
/// sql_rewriter.py remains the production version.
#[pyfunction]
#[pyo3(text_signature = "(sql)")]
fn extract_temporal_filters(py: Python, sql: String) -> PyResult<Py<PyAny>> {
    let result = extract_temporal_for_clauses(&sql);
    let pythonized = pythonize(py, &result).map_err(|e| {
        let msg = e.to_string();
        PyValueError::new_err(format!("Serialization failed.\n\t{msg}"))
    })?;
    Ok(pythonized.into())
}

/// Fast regex replacement using Rust's regex crate.
/// 
/// This function performs regex replacement on arrays of strings or bytes,
/// compiling the pattern once and applying it to all items efficiently.
/// 
/// Arguments:
/// - data: List of strings or bytes to process
/// - pattern: Regex pattern (string or bytes)
/// - replacement: Replacement string (string or bytes)
/// 
/// Returns:
/// - List of strings or bytes with replacements applied
fn regex_replace_rust(
    py: Python,
    data: Vec<Option<Py<PyAny>>>,
    pattern: Py<PyAny>,
    replacement: Py<PyAny>,
) -> PyResult<Vec<Option<Py<PyAny>>>> {
    // Currently a stub implementation for the regex PoC.
    // Full implementation requires FromPyObject handling which we
    // will reintroduce after stabilizing the IO PoC. Return an
    // empty vector for now.
    Ok(Vec::new())
}


#[pymodule]
fn compute(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_sql, m)?)?;
    m.add_function(wrap_pyfunction!(extract_temporal_filters, m)?)?;
    // `regex_replace_rust` is currently kept internal (not exposed)
    // to reduce PyO3 surface area during the IO PoC iteration.
    Ok(())
}