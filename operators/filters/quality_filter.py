"""
Quality Filter

Filters records based on quality criteria.
"""

from typing import Any

from framework import Filter


class QualityFilter(Filter):
    """Filter records based on quality metrics."""

    def __init__(
        self,
        min_width: int = 256,
        min_height: int = 256,
        max_compression_artifacts: float = 0.8,
        min_information_entropy: float = 3.0,
    ):
        super().__init__()
        self.min_width = min_width
        self.min_height = min_height
        self.max_compression_artifacts = max_compression_artifacts
        self.min_information_entropy = min_information_entropy

    def should_keep_batch(self, records: list[dict[str, Any]]) -> list[bool]:
        """Determine which records meet quality criteria."""
        results = []
        for record in records:
            width = record.get("width", 0)
            height = record.get("height", 0)
            compression_artifacts = record.get("compression_artifacts", 0.0)
            information_entropy = record.get("information_entropy", 0.0)

            keep = (
                width >= self.min_width
                and height >= self.min_height
                and compression_artifacts <= self.max_compression_artifacts
                and information_entropy >= self.min_information_entropy
            )
            results.append(keep)
        return results
