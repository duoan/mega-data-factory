"""
Visual Degradations Refiner

Assesses visual degradations using the multi-head quality assessment model.
This is a Refiner that enriches records with visual quality scores based on
the Z-Image paper's approach to technical quality assessment.

Degradation factors assessed:
- Color cast: Abnormal color tints
- Blurriness: Lack of sharpness/focus
- Watermark: Visible watermarks
- Noise: Visual noise levels

Reference: Z-Image Technical Report (Section 2.1 - Data Profiling Engine)
"""

from typing import Any

import pyarrow as pa

from framework import Refiner


class VisualDegradationsRefiner(Refiner):
    """Refiner for model-based visual degradations assessment.

    Uses a multi-head neural network to score images on multiple degradation factors:
    - color_cast: Color cast/tint level (0-1, higher = more color cast)
    - blurriness: Blur level (0-1, higher = more blurry)
    - watermark: Watermark visibility (0-1, higher = more visible)
    - noise: Noise level (0-1, higher = more noise)
    - visual_degradations_overall: Overall quality (0-1, higher = better quality)
    """

    def __init__(
        self,
        model_path: str | None = None,
        device: str | None = None,
        input_size: tuple[int, int] = (224, 224),
    ):
        """Initialize with model path.

        Args:
            model_path: Path to trained multi-head model file
            device: Device to use ('cpu', 'cuda', 'mps', or None for auto)
            input_size: Expected input size (H, W) for the model
        """
        super().__init__()
        self.model_path = model_path
        self.input_size = input_size
        self._device = device
        self._inference = None
        self._model_loaded = False

    def _ensure_model_loaded(self) -> bool:
        """Ensure the model is loaded (lazy loading).

        Returns:
            True if model is available, False otherwise
        """
        if self._model_loaded:
            return self._inference is not None

        self._model_loaded = True

        if self.model_path is None:
            return False

        try:
            from models.quality_assessment.inference import (
                MultiHeadQualityInference,
                get_auto_device,
            )

            device = self._device or get_auto_device()
            self._inference = MultiHeadQualityInference(
                model_path=self.model_path,
                device=device,
                input_size=self.input_size,
            )
            return True
        except Exception as e:
            print(f"Warning: Failed to load visual degradations model: {e}")
            return False

    def refine(self, record: dict[str, Any]) -> dict[str, Any]:
        """Assess visual degradations using model and return new fields.

        Args:
            record: Input record with image data

        Returns:
            Dictionary with degradation scores
        """
        # Default values if model not available or error occurs
        default_result = {
            "color_cast": None,
            "blurriness": None,
            "watermark": None,
            "noise": None,
            "visual_degradations_overall": None,
        }

        if not self._ensure_model_loaded():
            return default_result

        # Extract image bytes
        img_obj = record.get("image", {})
        if isinstance(img_obj, dict) and "bytes" in img_obj:
            image_bytes = img_obj["bytes"]
        else:
            return default_result

        try:
            scores = self._inference.predict_from_bytes(image_bytes)
            return {
                "color_cast": scores.color_cast,
                "blurriness": scores.blurriness,
                "watermark": scores.watermark,
                "noise": scores.noise,
                "visual_degradations_overall": scores.overall,
            }
        except Exception:
            return default_result

    def refine_batch(self, records: list[dict[str, Any]]) -> None:
        """Refine a batch of records inplace (optimized batch processing).

        Args:
            records: List of records to refine
        """
        if not records:
            return

        if not self._ensure_model_loaded():
            # Set default values
            for record in records:
                record["color_cast"] = None
                record["blurriness"] = None
                record["watermark"] = None
                record["noise"] = None
                record["visual_degradations_overall"] = None
            return

        # Collect valid image bytes
        valid_indices = []
        image_bytes_list = []

        for i, record in enumerate(records):
            img_obj = record.get("image", {})
            if isinstance(img_obj, dict) and "bytes" in img_obj:
                valid_indices.append(i)
                image_bytes_list.append(img_obj["bytes"])

        # Set defaults for all records first
        for record in records:
            record["color_cast"] = None
            record["blurriness"] = None
            record["watermark"] = None
            record["noise"] = None
            record["visual_degradations_overall"] = None

        if not image_bytes_list:
            return

        try:
            # Batch prediction
            scores_list = self._inference.predict_batch_from_bytes(image_bytes_list)

            # Update records with predictions
            for idx, scores in zip(valid_indices, scores_list, strict=False):
                records[idx]["color_cast"] = scores.color_cast
                records[idx]["blurriness"] = scores.blurriness
                records[idx]["watermark"] = scores.watermark
                records[idx]["noise"] = scores.noise
                records[idx]["visual_degradations_overall"] = scores.overall

        except Exception as e:
            print(f"Warning: Batch prediction failed: {e}")

    def get_output_schema(self) -> dict[str, pa.DataType]:
        """Return output schema for new fields added by this refiner."""
        return {
            "color_cast": pa.float32(),
            "blurriness": pa.float32(),
            "watermark": pa.float32(),
            "noise": pa.float32(),
            "visual_degradations_overall": pa.float32(),
        }
