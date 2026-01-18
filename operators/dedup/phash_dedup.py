"""
Perceptual Hash Deduplication

Deduplicates records based on perceptual hash (phash).
Auto-uses Rust backend (faster) if available, otherwise falls back to imagehash.
"""

from io import BytesIO
from typing import Any

from framework import Deduplicator

# Try to load Rust phash functions (faster)
RUST_PHASH_AVAILABLE = False
_compute_phash_rust = None
_compute_phash_batch_rust = None

try:
    import rust_accelerated_ops as _rust_module

    _compute_phash_rust = getattr(_rust_module, "compute_phash", None)
    _compute_phash_batch_rust = getattr(_rust_module, "compute_phash_batch", None)
    if _compute_phash_rust is not None:
        RUST_PHASH_AVAILABLE = True
except ImportError:
    pass

# Fallback imports (only if Rust not available)
if not RUST_PHASH_AVAILABLE:
    import imagehash
    from PIL import Image


class PhashDeduplicator(Deduplicator):
    """Deduplicates records based on perceptual hash.

    Auto-uses Rust backend (faster) if available, otherwise falls back to imagehash.
    """

    def __init__(self, hash_size: int = 16):
        super().__init__()
        self.hash_size = hash_size

    def get_dedup_keys_batch(self, records: list[dict[str, Any]]) -> list[str]:
        """Extract perceptual hashes from a batch of records."""
        # Separate records: those with existing phash vs those needing computation
        needs_compute_indices = []
        image_bytes_list = []

        for idx, record in enumerate(records):
            if "phash" not in record:
                img_obj = record.get("image", {})
                if isinstance(img_obj, dict) and "bytes" in img_obj:
                    needs_compute_indices.append(idx)
                    image_bytes_list.append(img_obj["bytes"])

        # Batch compute phashes with Rust if available
        computed_phashes = []
        if image_bytes_list and RUST_PHASH_AVAILABLE and _compute_phash_batch_rust:
            try:
                computed_phashes = _compute_phash_batch_rust(image_bytes_list, self.hash_size)
            except Exception:
                computed_phashes = []

        # Fallback: compute individually if Rust batch failed
        if image_bytes_list and len(computed_phashes) != len(image_bytes_list):
            computed_phashes = []
            for img_bytes in image_bytes_list:
                try:
                    if RUST_PHASH_AVAILABLE and _compute_phash_rust:
                        computed_phashes.append(_compute_phash_rust(img_bytes, self.hash_size))
                    else:
                        img = Image.open(BytesIO(img_bytes))
                        computed_phashes.append(str(imagehash.phash(img, hash_size=self.hash_size)))
                except Exception:
                    computed_phashes.append("")

        # Build keys list
        keys = []
        compute_idx = 0
        for idx, record in enumerate(records):
            if "phash" in record:
                keys.append(record["phash"])
            elif idx in needs_compute_indices:
                phash = computed_phashes[compute_idx] if compute_idx < len(computed_phashes) else ""
                keys.append(phash if phash else record.get("id", "unknown"))
                compute_idx += 1
            else:
                keys.append(record.get("id", "unknown"))

        return keys
