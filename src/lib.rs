use pythonize::pythonize;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use sqlparser::parser::Parser;
use regex::bytes::Regex as BytesRegex;
use regex::Regex;

mod opteryx_dialect;
pub use opteryx_dialect::OpteryxDialect;

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
        
        // Replacement can be either bytes or string - try both
        let replacement_str = if let Ok(bytes) = replacement.extract::<&[u8]>(py) {
            std::str::from_utf8(bytes).map_err(|e| {
                PyValueError::new_err(format!("Invalid UTF-8 in replacement: {}", e))
            })?.to_string()
        } else if let Ok(s) = replacement.extract::<String>(py) {
            s
        } else {
            return Err(PyValueError::new_err("Replacement must be bytes or string"));
        };
        
        // Convert Python-style backreferences (\1, \2, etc.) to Rust-style ($1, $2, etc.)
        let rust_replacement = convert_python_to_rust_backrefs(&replacement_str);
        
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
                    let replaced = re.replace_all(item_bytes, rust_replacement.as_bytes());
                    result.push(Some(PyBytes::new(py, &replaced).into()));
                }
            }
        }
        Ok(result)
    } else {
        // String mode - use string regex
        let pattern_str: String = pattern.extract(py)?;
        let replacement_str: String = replacement.extract(py)?;
        
        // Convert Python-style backreferences to Rust-style
        let rust_replacement = convert_python_to_rust_backrefs(&replacement_str);
        
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
                        let replaced = re.replace_all(item_str, &rust_replacement);
                        result.push(Some(PyBytes::new(py, replaced.as_bytes()).into()));
                    } else {
                        // Item is string
                        let item_str: String = item.extract(py)?;
                        let replaced = re.replace_all(&item_str, &rust_replacement);
                        result.push(Some(PyBytes::new(py, replaced.as_bytes()).into()));
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