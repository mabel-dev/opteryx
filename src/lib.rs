use pythonize::pythonize;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use regex::bytes::Regex;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::OnceLock;
use pyo3::types::PyList;
use pyo3::types::PyBytes;

mod opteryx_dialect;
pub use opteryx_dialect::OpteryxDialect;


/// 🚀 A simple global regex cache to avoid recompiling patterns
struct RegexCache {
    compiled: RwLock<HashMap<Vec<u8>, Regex>>,
}

fn get_regex_cache() -> &'static RegexCache {
    REGEX_CACHE.get_or_init(|| RegexCache::new())
}

impl RegexCache {
    fn new() -> Self {
        RegexCache {
            compiled: RwLock::new(HashMap::new()),
        }
    }

    fn get_or_compile(&self, pattern: &[u8]) -> PyResult<Regex> {
        let key = pattern.to_vec(); // Convert to owned Vec<u8> for storage

        {
            let cache = self.compiled.read().unwrap();
            if let Some(regex) = cache.get(&key) {
                return Ok(regex.clone()); // Use cached regex
            }
        }

        let pattern_str = std::str::from_utf8(&key)
            .map_err(|_| PyValueError::new_err("Invalid UTF-8 in pattern"))?;
        let regex = Regex::new(pattern_str)
            .map_err(|e| PyValueError::new_err(format!("Invalid regex: {}", e)))?;

        let mut cache = self.compiled.write().unwrap();
        cache.insert(key, regex.clone());

        Ok(regex)
    }
}

// 🚀 Lazy-initialized static REGEX_CACHE
static REGEX_CACHE: OnceLock<RegexCache> = OnceLock::new();

/// 🚀 Fast regex match using precompiled patterns
#[pyfunction]
fn regex_match(pattern: &[u8], text: &[u8]) -> PyResult<bool> {
    let regex = get_regex_cache().get_or_compile(pattern)?;
    Ok(regex.is_match(text))
}


/// 🚀 Efficiently process a NumPy array of bytes using regex replace
#[pyfunction]
fn regex_replace_batch(py: Python, pattern: &[u8], replacement: &[u8], texts: &PyList) -> PyResult<Vec<PyObject>> {
    let regex = Regex::new(std::str::from_utf8(pattern).unwrap()).unwrap();

    let mut results = Vec::with_capacity(texts.len()?);

    for text in texts.iter() {
        let text_bytes: &[u8] = text.extract()?;
        let result = regex.replace_all(text_bytes, replacement).into_owned();
        results.push(PyBytes::new(py, &result).into());
    }

    Ok(results)
}


/// Function to parse SQL statements from a string.
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

/// Register all functions in the Python module
#[pymodule]
fn compute(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_sql, m)?)?;
    m.add_function(wrap_pyfunction!(regex_match, m)?)?;
    m.add_function(wrap_pyfunction!(regex_replace_batch, m)?)?;
    Ok(())
}