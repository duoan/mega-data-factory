use image::RgbImage;
use image_hasher::{HashAlg, HasherConfig};
use pyo3::prelude::*;
use rayon::prelude::*;

/// Calculate Shannon entropy for a single channel (optimized with lookup table)
fn channel_entropy(channel: &[u8]) -> f64 {
    let mut counts = [0u32; 256];
    let total = channel.len() as u32;

    for &pixel in channel {
        counts[pixel as usize] += 1;
    }

    if total == 0 {
        return 0.0;
    }

    let inv_total = 1.0 / total as f64;
    let mut entropy = 0.0;
    for &count in counts.iter() {
        if count > 0 {
            let prob = count as f64 * inv_total;
            entropy -= prob * prob.log2();
        }
    }

    entropy
}

/// Calculate information entropy directly from RGB image (no extra allocation for channels)
fn calculate_entropy_from_rgb(rgb_img: &RgbImage) -> f64 {
    let mut r_counts = [0u32; 256];
    let mut g_counts = [0u32; 256];
    let mut b_counts = [0u32; 256];
    let mut total = 0u32;

    for pixel in rgb_img.pixels() {
        r_counts[pixel[0] as usize] += 1;
        g_counts[pixel[1] as usize] += 1;
        b_counts[pixel[2] as usize] += 1;
        total += 1;
    }

    if total == 0 {
        return 0.0;
    }

    let inv_total = 1.0 / total as f64;

    let calc_entropy = |counts: &[u32; 256]| -> f64 {
        let mut entropy = 0.0;
        for &count in counts.iter() {
            if count > 0 {
                let prob = count as f64 * inv_total;
                entropy -= prob * prob.log2();
            }
        }
        entropy
    };

    let r_entropy = calc_entropy(&r_counts);
    let g_entropy = calc_entropy(&g_counts);
    let b_entropy = calc_entropy(&b_counts);

    (r_entropy + g_entropy + b_entropy) / 3.0
}

/// Detect compression artifacts using pre-converted RGB image
fn detect_compression_artifacts_from_rgb(rgb_img: &RgbImage, image_bytes_len: usize) -> f64 {
    let (width, height) = rgb_img.dimensions();

    // Calculate blockiness (JPEG 8x8 block boundaries)
    let block_size = 8;
    let max_samples = 64; // Limit boundary checks for performance

    let h_block_count = ((height.saturating_sub(1)) / block_size).min(max_samples as u32);
    let w_block_count = ((width.saturating_sub(1)) / block_size).min(max_samples as u32);

    let mut v_sum = 0u64;
    let mut v_count = 0u32;
    let mut h_sum = 0u64;
    let mut h_count = 0u32;

    // Vertical boundaries (horizontal differences at block boundaries)
    for i in 0..h_block_count {
        let y = i * block_size;
        if y + 1 < height {
            let mut sum = 0u32;
            for x in 0..width {
                let p1 = rgb_img.get_pixel(x, y);
                let p2 = rgb_img.get_pixel(x, y + 1);
                // Fast grayscale: (r + g + b) / 3, using integer division
                let gray1 = (p1[0] as u16 + p1[1] as u16 + p1[2] as u16) / 3;
                let gray2 = (p2[0] as u16 + p2[1] as u16 + p2[2] as u16) / 3;
                let diff = (gray1 as i16 - gray2 as i16).unsigned_abs() as u32;
                sum += diff;
            }
            v_sum += (sum / width) as u64;
            v_count += 1;
        }
    }

    // Horizontal boundaries (vertical differences at block boundaries)
    for j in 0..w_block_count {
        let x = j * block_size;
        if x + 1 < width {
            let mut sum = 0u32;
            for y in 0..height {
                let p1 = rgb_img.get_pixel(x, y);
                let p2 = rgb_img.get_pixel(x + 1, y);
                let gray1 = (p1[0] as u16 + p1[1] as u16 + p1[2] as u16) / 3;
                let gray2 = (p2[0] as u16 + p2[1] as u16 + p2[2] as u16) / 3;
                let diff = (gray1 as i16 - gray2 as i16).unsigned_abs() as u32;
                sum += diff;
            }
            h_sum += (sum / height) as u64;
            h_count += 1;
        }
    }

    let blockiness = if v_count > 0 && h_count > 0 {
        let v_avg = v_sum as f64 / v_count as f64 / 255.0;
        let h_avg = h_sum as f64 / h_count as f64 / 255.0;
        (v_avg + h_avg) / 2.0
    } else {
        0.0
    };

    // Compression ratio score
    let uncompressed_size = (width * height * 3) as usize;
    let compressed_ratio = if uncompressed_size > 0 {
        image_bytes_len as f64 / uncompressed_size as f64
    } else {
        1.0
    };
    let compression_score = 1.0 - (compressed_ratio * 2.0).min(1.0);

    // Combined score (60% blockiness, 40% compression)
    let artifact_score = blockiness * 0.6 + compression_score * 0.4;
    artifact_score.clamp(0.0, 1.0)
}

/// Core function to process single image (decode once, compute both metrics)
fn assess_quality_core(image_bytes: &[u8]) -> (f64, f64) {
    match image::load_from_memory(image_bytes) {
        Ok(img) => {
            // Convert to RGB8 only once
            let rgb_img = img.to_rgb8();
            let compression_artifacts =
                detect_compression_artifacts_from_rgb(&rgb_img, image_bytes.len());
            let entropy = calculate_entropy_from_rgb(&rgb_img);
            (compression_artifacts, entropy)
        }
        Err(_) => (0.0, 0.0),
    }
}

/// Process image bytes and return compression artifacts and entropy
#[pyfunction]
fn assess_quality(image_bytes: &[u8]) -> PyResult<(f64, f64)> {
    Ok(assess_quality_core(image_bytes))
}

/// Batch process multiple images in parallel using rayon
/// This significantly speeds up processing on multi-core systems
#[pyfunction]
fn assess_quality_batch(image_bytes_list: Vec<Vec<u8>>) -> PyResult<Vec<(f64, f64)>> {
    // Use rayon for parallel processing
    let results: Vec<(f64, f64)> = image_bytes_list
        .par_iter()
        .map(|image_bytes| assess_quality_core(image_bytes))
        .collect();

    Ok(results)
}

// ============================================================================
// Perceptual Hash (phash) functions
// ============================================================================

/// Compute perceptual hash for a single image
fn compute_phash_core(image_bytes: &[u8], hash_size: u32) -> Option<String> {
    let img = image::load_from_memory(image_bytes).ok()?;

    let hasher = HasherConfig::new()
        .hash_size(hash_size, hash_size)
        .hash_alg(HashAlg::DoubleGradient) // Similar to imagehash.phash
        .to_hasher();

    let hash = hasher.hash_image(&img);
    Some(hash.to_base64())
}

/// Compute perceptual hash for a single image
/// Returns base64-encoded hash string (more compact than hex)
#[pyfunction]
#[pyo3(signature = (image_bytes, hash_size=16))]
fn compute_phash(image_bytes: &[u8], hash_size: u32) -> PyResult<String> {
    compute_phash_core(image_bytes, hash_size)
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Failed to compute phash"))
}

/// Batch compute perceptual hashes in parallel
/// Returns list of base64-encoded hash strings (empty string for errors)
#[pyfunction]
#[pyo3(signature = (image_bytes_list, hash_size=16))]
fn compute_phash_batch(image_bytes_list: Vec<Vec<u8>>, hash_size: u32) -> PyResult<Vec<String>> {
    let results: Vec<String> = image_bytes_list
        .par_iter()
        .map(|image_bytes| compute_phash_core(image_bytes, hash_size).unwrap_or_default())
        .collect();

    Ok(results)
}

#[pymodule]
fn rust_accelerated_ops(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(assess_quality, m)?)?;
    m.add_function(wrap_pyfunction!(assess_quality_batch, m)?)?;
    m.add_function(wrap_pyfunction!(compute_phash, m)?)?;
    m.add_function(wrap_pyfunction!(compute_phash_batch, m)?)?;
    Ok(())
}
