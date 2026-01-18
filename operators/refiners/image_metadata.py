"""
Image Metadata Refiner

Extracts basic image metadata: width, height, file size, format.
This is a Refiner that enriches records with metadata information.
"""

from io import BytesIO
from typing import Any

import pyarrow as pa
from PIL import Image

from framework import Refiner


class ImageMetadataRefiner(Refiner):
    """Refiner for extracting basic image metadata."""

    def refine_batch(self, records: list[dict[str, Any]]) -> None:
        """Extract basic image metadata for a batch of records (inplace)."""
        for record in records:
            img_obj = record.get("image", {})

            if isinstance(img_obj, dict) and "bytes" in img_obj:
                image_bytes = img_obj["bytes"]
                try:
                    img = Image.open(BytesIO(image_bytes))
                    w, h = img.size
                    record["width"] = w
                    record["height"] = h
                    record["file_size_bytes"] = len(image_bytes)
                    record["format"] = img.format or "UNKNOWN"
                except Exception:
                    record["width"] = 0
                    record["height"] = 0
                    record["file_size_bytes"] = len(image_bytes)
                    record["format"] = "ERROR"
            else:
                record["width"] = 0
                record["height"] = 0
                record["file_size_bytes"] = 0
                record["format"] = "ERROR"

    def get_output_schema(self) -> dict[str, pa.DataType]:
        """Return output schema for new fields added by this refiner."""
        return {
            "width": pa.int32(),
            "height": pa.int32(),
            "file_size_bytes": pa.int64(),
            "format": pa.string(),
        }
