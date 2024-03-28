use numpy::{PyArray1, PyArray2, IntoPyArray};
use pyo3::{Python, PyResult, prelude::*};


#[pyfunction]
pub fn anyop_eq_numeric(py: Python<'_>, literal: i64, arr: &PyArray2<i64>) -> PyResult<Py<PyArray1<bool>>> {
    let array = unsafe { arr.as_array() };
    let result = array.map_axis(ndarray::Axis(1), |row| {
        row.iter().any(|&item| item == literal)
    });
    Ok(result.into_pyarray(py).to_owned())
}


use pyo3::types::{PyAny, PyString};

#[pyfunction]
pub fn anyop_eq_string(_py: Python, value: &str, arr: &PyAny) -> PyResult<Vec<bool>> {
    // Assume `arr` is a 2D array-like object (e.g., numpy array or list of lists)
    let rows = arr.getattr("shape")?.extract::<(usize, )>()?.0;
    let mut results = Vec::new();

    for i in 0..rows {
        let row = arr.get_item((i,))?;
        let mut found = false;
        
        // Assuming `row` can be iterated over, reflecting a sequence of strings.
        for item in row.iter()? {
            let item_str = item?.downcast::<PyString>()?.to_str()?;
            if item_str == value {
                found = true;
                break;
            }
        }
        
        results.push(found);
    }

    Ok(results)
}
