//! Text processing operators: HTML extraction, WARC parsing
//!
//! Provides Rust-accelerated text operations:
//! - `html_extract_text_batch`: Extract readable text from HTML using readability algorithm
//! - `warc_extract_records`: Parse WARC.gz file and extract text in one pass (streaming)
//! - `warc_extract_records_batch`: Parallel processing of multiple WARC files

use dom_smoothie::Readability;
use pyo3::prelude::*;
use rayon::prelude::*;
use warc::{WarcHeader, WarcReader};

// ============================================================================
// HTML Text Extraction
// ============================================================================

/// Extract readable text from HTML using dom_smoothie (Rust port of readability.js)
fn html_extract_text_core(html: &str) -> Option<(String, String)> {
    let mut readability = Readability::new(html, None, None).ok()?;
    let article = readability.parse().ok()?;

    let title = article.title;
    let content = article.text_content.to_string();

    // Skip if content is empty or too short
    if content.trim().is_empty() || content.len() < 50 {
        return None;
    }

    Some((title, content))
}

// ============================================================================
// WARC Streaming - parse + extract in one pass
// ============================================================================

/// Parsed WARC record with extracted content
struct ParsedRecord {
    url: String,
    date: String,
    html: String,
}

/// Extract text from WARC file: streaming I/O + parallel text extraction
/// Returns Vec of (url, date, title, text, text_length) tuples
#[pyfunction]
pub fn warc_extract_records(
    warc_path: String,
) -> PyResult<Vec<(String, String, String, String, usize)>> {
    let mut warc_reader = match WarcReader::from_path_gzip(&warc_path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[WARC] Failed to open {}: {}", warc_path, e);
            return Ok(Vec::new());
        }
    };

    // Phase 1: Stream and filter WARC records (single-threaded I/O)
    let mut html_records = Vec::new();

    let mut iter = warc_reader.stream_records();
    while let Some(result) = iter.next_item() {
        let record = match result {
            Ok(r) => r,
            Err(_) => continue,
        };

        // Check WARC type from header (body not loaded yet)
        let warc_type = record
            .header(WarcHeader::WarcType)
            .map(|v| v.to_string())
            .unwrap_or_default();

        if warc_type != "response" {
            // Skip: buffer body to advance stream
            let _ = record.into_buffered();
            continue;
        }

        // Get URL and date from headers
        let url = record
            .header(WarcHeader::TargetURI)
            .map(|v| v.to_string())
            .unwrap_or_default();

        let date = record
            .header(WarcHeader::Date)
            .map(|v| v.to_string())
            .unwrap_or_default();

        // Now buffer the body (lazy load)
        let buffered = match record.into_buffered() {
            Ok(b) => b,
            Err(_) => continue,
        };

        let body = buffered.body();
        let body_str = String::from_utf8_lossy(body);

        // Split HTTP headers from body
        let parts: Vec<&str> = body_str.splitn(2, "\r\n\r\n").collect();
        if parts.len() < 2 {
            continue;
        }

        let http_headers = parts[0].to_lowercase();
        let html_body = parts[1];

        // Check if it's HTML
        if !http_headers.contains("content-type: text/html")
            && !http_headers.contains("content-type:text/html")
        {
            continue;
        }

        if html_body.is_empty() {
            continue;
        }

        html_records.push(ParsedRecord {
            url,
            date,
            html: html_body.to_string(),
        });
    }

    if html_records.is_empty() {
        return Ok(Vec::new());
    }

    // Phase 2: Parallel text extraction using rayon
    let results: Vec<_> = html_records
        .into_par_iter()
        .filter_map(|record| {
            let (title, text) = html_extract_text_core(&record.html)?;
            if text.is_empty() {
                return None;
            }
            let text_len = text.len();
            Some((record.url, record.date, title, text, text_len))
        })
        .collect();

    Ok(results)
}

/// Batch extract from multiple WARC files in parallel
/// Each file uses streaming parse + extract
#[pyfunction]
pub fn warc_extract_records_batch(
    warc_paths: Vec<String>,
) -> PyResult<Vec<Vec<(String, String, String, String, usize)>>> {
    let results: Vec<Vec<_>> = warc_paths
        .into_par_iter()
        .map(|path| {
            warc_extract_records(path).unwrap_or_default()
        })
        .collect();

    Ok(results)
}
