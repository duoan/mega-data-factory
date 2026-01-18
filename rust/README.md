# Rust Accelerated Operators

Rust-accelerated backend for image processing operations (3-10x faster than Python).

## Features

- **assess_quality / assess_quality_batch**: Technical quality assessment (compression artifacts, entropy)
- **compute_phash / compute_phash_batch**: Perceptual hash computation

All batch functions use Rayon for parallel processing on multi-core systems.

## Building

```bash
cd rust
./build.sh
```

Or manually:

```bash
cd rust
maturin build --release -o ../dist
uv pip install ../dist/rust_accelerated_ops-*.whl
```

## Testing

```bash
python rust/test.py
```

## Usage

The operators automatically detect and use this Rust extension if installed:

- `TechnicalQualityRefiner` in `operators/refiners/technical_quality.py`
- `PhashDeduplicator` in `operators/dedup/phash_dedup.py`

## Performance

Expected speedup compared to Python/numpy implementation:

| Operation | Python | Rust | Speedup |
|-----------|--------|------|---------|
| assess_quality | 3.3ms | 0.6ms | ~5x |
| compute_phash | 1.5ms | 1.0ms | ~1.5x |
| batch (parallel) | N/A | uses all cores | N*x |

## Dependencies

- `pyo3` - Python bindings
- `image` - Image decoding
- `rayon` - Parallel processing
- `image_hasher` - Perceptual hashing
