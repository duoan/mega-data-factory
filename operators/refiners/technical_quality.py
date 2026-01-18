"""
Technical Quality Refiner (Auto-accelerated with Rust if available)

Assesses technical quality metrics:
- Compression artifacts detection
- Information entropy calculation
This is a Refiner that enriches records with quality metrics.

Automatically uses Rust backend (3-10x faster) if available, otherwise falls back to Python implementation.
"""

import importlib.util
import shutil
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
from PIL import Image

from framework import Refiner

# Try to load Rust extension (auto-acceleration)
# Priority: 1. Installed package, 2. Local .so file
RUST_BACKEND_AVAILABLE = False
_assess_quality_rust = None
_assess_quality_batch_rust = None

try:
    # First, try to import the installed package (preferred)
    import rust_accelerated_ops as _rust_module

    _assess_quality_rust = _rust_module.assess_quality
    _assess_quality_batch_rust = getattr(_rust_module, "assess_quality_batch", None)
    RUST_BACKEND_AVAILABLE = True
except ImportError:
    # Fallback: try to load from local build
    try:
        rust_lib_dir = Path(__file__).parent.parent.parent / "rust" / "target" / "release"
        so_path = rust_lib_dir / "technical_quality.so"

        # Ensure .so file exists (copy from .dylib if needed)
        if not so_path.exists():
            dylib_path = rust_lib_dir / "libtechnical_quality.dylib"
            if dylib_path.exists():
                shutil.copy(dylib_path, so_path)

        if so_path.exists():
            spec = importlib.util.spec_from_file_location("technical_quality", so_path)
            if spec and spec.loader:
                _rust_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(_rust_module)
                _assess_quality_rust = _rust_module.assess_quality
                _assess_quality_batch_rust = getattr(_rust_module, "assess_quality_batch", None)
                RUST_BACKEND_AVAILABLE = True
    except (ImportError, OSError):
        pass


class TechnicalQualityRefiner(Refiner):
    """Refiner for technical quality assessment (compression artifacts, entropy).

    Automatically uses Rust backend (3-10x faster) if available, otherwise falls back to Python.
    """

    def refine_batch(self, records: list[dict[str, Any]]) -> None:
        """Refine a batch of records inplace (optimized with Rust batch processing)."""
        if not records:
            return

        # Use Rust batch function if available (much faster)
        if RUST_BACKEND_AVAILABLE and _assess_quality_batch_rust:
            try:
                image_bytes_list = [
                    record.get("image", {}).get("bytes", b"") if isinstance(record.get("image"), dict) else b""
                    for record in records
                ]
                batch_results = _assess_quality_batch_rust(image_bytes_list)

                for record, (ca, ent) in zip(records, batch_results, strict=False):
                    record["compression_artifacts"] = float(ca)
                    record["information_entropy"] = float(ent)
                return
            except Exception:
                pass  # Fallback to Python

        # Fallback to Python implementation
        for record in records:
            img_obj = record.get("image", {})
            if isinstance(img_obj, dict) and "bytes" in img_obj:
                try:
                    result = self._refine_python(img_obj["bytes"])
                    record["compression_artifacts"] = result["compression_artifacts"]
                    record["information_entropy"] = result["information_entropy"]
                except Exception:
                    record["compression_artifacts"] = 0.0
                    record["information_entropy"] = 0.0
            else:
                record["compression_artifacts"] = 0.0
                record["information_entropy"] = 0.0

    def _refine_python(self, image_bytes: bytes) -> dict[str, Any]:
        """Python fallback implementation (slower but always available)."""
        img = Image.open(BytesIO(image_bytes))
        compression_artifacts = self._detect_compression_artifacts(img, image_bytes)
        entropy = self._calculate_entropy(img)

        return {"compression_artifacts": compression_artifacts, "information_entropy": entropy}

    def _detect_compression_artifacts(
        self,
        img: Image.Image,
        image_bytes: bytes,
    ) -> float:
        """Detect compression artifacts (0-1, higher = more artifacts)."""
        if img.mode != "RGB":
            img = img.convert("RGB")

        # Use uint8 instead of float32 to reduce memory usage
        img_array = np.array(img, dtype=np.uint8)
        # Use integer mean to avoid float conversion overhead
        gray = img_array.mean(axis=2, dtype=np.uint8)

        # Optimize blockiness detection using numpy vectorization
        block_size = 8
        h, w = gray.shape

        # Sample block boundaries instead of checking all (reduce computation)
        # For small images, use all boundaries; for large images, sample
        max_samples = 64  # Limit number of boundary checks

        h_block_indices = np.arange(0, h - 1, block_size)[:max_samples]
        w_block_indices = np.arange(0, w - 1, block_size)[:max_samples]

        if len(h_block_indices) > 0 and len(w_block_indices) > 0:
            # Vectorized boundary detection
            v_diff = np.abs(np.diff(gray, axis=0))
            h_diff = np.abs(np.diff(gray, axis=1))

            v_boundaries = v_diff[h_block_indices, :].mean(axis=1) if len(h_block_indices) > 0 else np.array([])
            h_boundaries = h_diff[:, w_block_indices].mean(axis=0) if len(w_block_indices) > 0 else np.array([])

            if len(v_boundaries) > 0 and len(h_boundaries) > 0:
                h_score = v_boundaries.mean() / 255.0
                v_score = h_boundaries.mean() / 255.0
                blockiness = (h_score + v_score) / 2.0
            else:
                blockiness = 0.0
        else:
            blockiness = 0.0

        # Check compression ratio (optimized calculation)
        w_img, h_img = img.size
        uncompressed_size = w_img * h_img * 3
        compressed_ratio = len(image_bytes) / uncompressed_size if uncompressed_size > 0 else 1.0
        compression_score = 1.0 - min(1.0, compressed_ratio * 2.0)

        artifact_score = blockiness * 0.6 + compression_score * 0.4
        return min(1.0, max(0.0, artifact_score))

    def _calculate_entropy(self, img: Image.Image) -> float:
        """Calculate Shannon information entropy."""
        if img.mode == "RGB":
            img_array = np.array(img)
            r_entropy = self._channel_entropy(img_array[:, :, 0])
            g_entropy = self._channel_entropy(img_array[:, :, 1])
            b_entropy = self._channel_entropy(img_array[:, :, 2])
            overall_entropy = (r_entropy + g_entropy + b_entropy) / 3.0
        elif img.mode == "L":
            img_array = np.array(img)
            overall_entropy = self._channel_entropy(img_array)
        else:
            img_rgb = img.convert("RGB")
            img_array = np.array(img_rgb)
            r_entropy = self._channel_entropy(img_array[:, :, 0])
            g_entropy = self._channel_entropy(img_array[:, :, 1])
            b_entropy = self._channel_entropy(img_array[:, :, 2])
            overall_entropy = (r_entropy + g_entropy + b_entropy) / 3.0

        return overall_entropy

    def _channel_entropy(self, channel: np.ndarray) -> float:
        """Calculate Shannon entropy for a single channel (optimized)."""
        # Use bincount for uint8 channels (faster than histogram)
        if channel.dtype == np.uint8:
            counts = np.bincount(channel.flatten(), minlength=256)
        else:
            # For other types, use histogram but ensure uint8 range
            counts, _ = np.histogram(channel.flatten(), bins=256, range=(0, 256))

        # Normalize and filter zeros
        total = counts.sum()
        if total == 0:
            return 0.0

        probs = counts[counts > 0] / total
        entropy = -np.sum(probs * np.log2(probs))
        return float(entropy)

    def get_output_schema(self) -> dict[str, pa.DataType]:
        """Return output schema for new fields added by this refiner."""
        return {
            "compression_artifacts": pa.float32(),
            "information_entropy": pa.float32(),
        }
