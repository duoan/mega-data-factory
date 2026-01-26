//! Mega Data Factory - Rust Accelerated Operators
//!
//! This crate provides high-performance operators for data processing:
//!
//! ## Image Operations (`image_ops`)
//! - `image_assess_quality_batch`: Compression artifacts + entropy calculation
//! - `image_compute_phash_batch`: Perceptual hash computation
//!
//! ## Text Operations (`text_ops`)
//! - `html_extract_text_batch`: Extract readable text from HTML
//! - `warc_extract_records`: Parse WARC.gz and extract text in one pass
//! - `warc_extract_records_batch`: Parallel WARC file processing

mod image_ops;
mod text_ops;

use pyo3::prelude::*;

// Re-export all public functions
pub use image_ops::{image_assess_quality_batch, image_compute_phash_batch};
pub use text_ops::{warc_extract_records, warc_extract_records_batch};

/// Python module definition
#[pymodule]
fn rust_operators(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Image operations
    m.add_function(wrap_pyfunction!(image_ops::image_assess_quality_batch, m)?)?;
    m.add_function(wrap_pyfunction!(image_ops::image_compute_phash_batch, m)?)?;

    // Text operations
    m.add_function(wrap_pyfunction!(text_ops::warc_extract_records, m)?)?;
    m.add_function(wrap_pyfunction!(text_ops::warc_extract_records_batch, m)?)?;

    Ok(())
}
