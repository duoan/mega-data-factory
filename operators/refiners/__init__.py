"""
Refiner operators package.

This package contains refiner implementations.
Refiners enrich records by adding new information.
Refiners are automatically registered when this package is imported.
"""

from framework import OperatorRegistry

from .image_clip_embedding import ImageClipEmbeddingRefiner
from .image_metadata import ImageMetadataRefiner
from .technical_quality import TechnicalQualityRefiner
from .visual_degradations import VisualDegradationsRefiner

# Register all refiners with the framework
OperatorRegistry.register("ImageMetadataRefiner", ImageMetadataRefiner)
OperatorRegistry.register("TechnicalQualityRefiner", TechnicalQualityRefiner)  # Auto-uses Rust if available
OperatorRegistry.register("VisualDegradationsRefiner", VisualDegradationsRefiner)
OperatorRegistry.register("ImageClipEmbeddingRefiner", ImageClipEmbeddingRefiner)

__all__ = [
    "ImageMetadataRefiner",
    "TechnicalQualityRefiner",
    "VisualDegradationsRefiner",
    "ImageClipEmbeddingRefiner",
]
