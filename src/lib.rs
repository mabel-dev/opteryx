use pythonize::pythonize;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::IntoPy;

use sqlparser::parser::Parser;
use regex::bytes::Regex as BytesRegex;
use regex::Regex;

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
#[pyfunction]
#[pyo3(text_signature = "(data, pattern, replacement)")]
fn regex_replace_rust(
    py: Python,
    data: Vec<Option<Py<PyAny>>>,
    pattern: Py<PyAny>,
    replacement: Py<PyAny>,
) -> PyResult<Vec<Option<Py<PyAny>>>> {
    // Check if we're working with bytes or strings
    let is_bytes = pattern.bind(py).is_instance_of::<PyBytes>();
    
    if is_bytes {
        // Bytes mode - use bytes regex
        let pattern_bytes: &[u8] = pattern.extract(py)?;
        let replacement_bytes: &[u8] = replacement.extract(py)?;
        
        // Compile regex once
        let re = BytesRegex::new(std::str::from_utf8(pattern_bytes).map_err(|e| {
            PyValueError::new_err(format!("Invalid UTF-8 in pattern: {}", e))
        })?)
        .map_err(|e| PyValueError::new_err(format!("Invalid regex pattern: {}", e)))?;
        
        // Process each item
        let mut result = Vec::with_capacity(data.len());
        for item_opt in data {
            match item_opt {
                None => result.push(None),
                Some(item) => {
                    let item_bytes: &[u8] = item.extract(py)?;
                    let replaced = re.replace_all(item_bytes, replacement_bytes);
                    result.push(Some(PyBytes::new(py, &replaced).into()));
                }
            }
        }
        Ok(result)
    } else {
        // String mode - use string regex
        let pattern_str: String = pattern.extract(py)?;
        let replacement_str: String = replacement.extract(py)?;
        
        // Compile regex once
        let re = Regex::new(&pattern_str)
            .map_err(|e| PyValueError::new_err(format!("Invalid regex pattern: {}", e)))?;
        
        // Process each item
        let mut result = Vec::with_capacity(data.len());
        for item_opt in data {
            match item_opt {
                None => result.push(None),
                Some(item) => {
                    if let Ok(item_bytes) = item.extract::<&[u8]>(py) {
                        // Item is bytes, convert to string, replace, convert back
                        let item_str = std::str::from_utf8(item_bytes)
                            .map_err(|e| PyValueError::new_err(format!("Invalid UTF-8: {}", e)))?;
                        let replaced = re.replace_all(item_str, &replacement_str);
                        result.push(Some(PyBytes::new(py, replaced.as_bytes()).into()));
                    } else {
                        // Item is string
                        let item_str: String = item.extract(py)?;
                        let replaced = re.replace_all(&item_str, &replacement_str);
                        result.push(Some(replaced.to_string().into_py(py)));
                    }
                }
            }
        }
        Ok(result)
    }
}


#[pymodule]
fn compute(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_sql, m)?)?;
    m.add_function(wrap_pyfunction!(regex_replace_rust, m)?)?;
    Ok(())
}
