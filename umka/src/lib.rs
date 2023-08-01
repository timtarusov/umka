mod fuzzywuzzy;

use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::error::PyPolarsErr;
use pyo3_polars::PyDataFrame;
#[pyfunction]
fn fuzz_ratio(pydf: PyDataFrame, cola: &str, colb: &str) -> PyResult<PyDataFrame> {
    let df: DataFrame = pydf.into();
    let df = fuzzywuzzy::pl_ratio(df, cola, colb).map_err(PyPolarsErr::from)?;
    Ok(PyDataFrame(df))
}

// #[pyfunction]
// fn lazy_fuzz_ratio(pydf: PyLazyFrame, cola: &str, colb: &str) -> PyResult<PyLazyFrame> {
//     let df: LazyFrame = pydf.into();
//     let df = fuzzywuzzy::pl_ratio(df.collect().unwrap(), cola, colb).map_err(PyPolarsErr::from)?;
//     Ok(PyLazyFrame(df.lazy()))
// }

#[pyfunction]
fn fuzz_partial_ratio(pydf: PyDataFrame, cola: &str, colb: &str) -> PyResult<PyDataFrame> {
    let df: DataFrame = pydf.into();
    let df = fuzzywuzzy::pl_partial_ratio(df, cola, colb).map_err(PyPolarsErr::from)?;
    Ok(PyDataFrame(df))
}

// #[pyfunction]
// fn lazy_fuzz_partial_ratio(pydf: PyLazyFrame, cola: &str, colb: &str) -> PyResult<PyLazyFrame> {
//     let df: LazyFrame = pydf.into();
//     let df = fuzzywuzzy::pl_partial_ratio(df.collect().unwrap(), cola, colb)
//         .map_err(PyPolarsErr::from)?;
//     Ok(PyLazyFrame(df.lazy()))
// }

#[pyfunction]
fn fuzz_token_set_ratio(pydf: PyDataFrame, cola: &str, colb: &str) -> PyResult<PyDataFrame> {
    let df: DataFrame = pydf.into();
    let df = fuzzywuzzy::pl_token_set_ratio(df, cola, colb).map_err(PyPolarsErr::from)?;
    Ok(PyDataFrame(df))
}

// #[pyfunction]
// fn lazy_fuzz_token_set_ratio(pydf: PyLazyFrame, cola: &str, colb: &str) -> PyResult<PyLazyFrame> {
//     let df: LazyFrame = pydf.into();
//     let df = fuzzywuzzy::pl_token_set_ratio(df.collect().unwrap(), cola, colb)
//         .map_err(PyPolarsErr::from)?;
//     Ok(PyLazyFrame(df.lazy()))
// }

#[pyfunction]
fn fuzz_token_sort_ratio(pydf: PyDataFrame, cola: &str, colb: &str) -> PyResult<PyDataFrame> {
    let df: DataFrame = pydf.into();
    let df = fuzzywuzzy::pl_token_sort_ratio(df, cola, colb).map_err(PyPolarsErr::from)?;
    Ok(PyDataFrame(df))
}

// #[pyfunction]
// fn lazy_fuzz_token_sort_ratio(pydf: PyLazyFrame, cola: &str, colb: &str) -> PyResult<PyLazyFrame> {
//     let df: LazyFrame = pydf.into();
//     let df = fuzzywuzzy::pl_token_sort_ratio(df.collect().unwrap(), cola, colb)
//         .map_err(PyPolarsErr::from)?;
//     Ok(PyLazyFrame(df.lazy()))
// }

#[pymodule]
#[pyo3(name = "umka_rs")]
fn umka_rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(fuzz_ratio, m)?)?;
    // m.add_function(wrap_pyfunction!(lazy_fuzz_ratio, m)?)?;
    m.add_function(wrap_pyfunction!(fuzz_partial_ratio, m)?)?;
    // m.add_function(wrap_pyfunction!(lazy_fuzz_partial_ratio, m)?)?;
    m.add_function(wrap_pyfunction!(fuzz_token_set_ratio, m)?)?;
    // m.add_function(wrap_pyfunction!(lazy_fuzz_token_set_ratio, m)?)?;
    m.add_function(wrap_pyfunction!(fuzz_token_sort_ratio, m)?)?;
    // m.add_function(wrap_pyfunction!(lazy_fuzz_token_sort_ratio, m)?)?;
    Ok(())
}
