"""
Text Length Filter

Filters records based on text length criteria.
"""

from typing import Any

from mega_data_factory.framework import Filter

FIELD_TEXT_LENGTH = "text_length"
FIELD_TEXT = "text"


class TextLengthFilter(Filter):
    """Filter records based on text length.

    If text_length field exists, uses it directly.
    Otherwise, calculates length from the text field.
    """

    def __init__(
        self,
        min_length: int = 0,
        max_length: int | None = None,
        text_field: str = FIELD_TEXT,
        text_length_field: str = FIELD_TEXT_LENGTH,
    ):
        """Initialize text length filter.

        Args:
            min_length: Minimum text length (inclusive). Default: 0
            max_length: Maximum text length (inclusive). None means no upper limit.
            text_field: Name of the text field to calculate length from.
            text_length_field: Name of the pre-computed text length field.
        """
        super().__init__()
        self.min_length = min_length
        self.max_length = max_length
        self.text_field = text_field
        self.text_length_field = text_length_field

    def _get_text_length(self, record: dict[str, Any]) -> int:
        """Get text length from record, using pre-computed field if available."""
        if self.text_length_field in record:
            length = record[self.text_length_field]
            if isinstance(length, (int, float)):
                return int(length)

        text = record.get(self.text_field)
        if text is None:
            return 0
        if isinstance(text, str):
            return len(text)
        if isinstance(text, bytes):
            return len(text)
        return 0

    def should_keep_batch(self, records: list[dict[str, Any]]) -> list[bool]:
        """Determine which records meet text length criteria."""
        results = []
        for record in records:
            length = self._get_text_length(record)

            keep = length >= self.min_length
            if self.max_length is not None:
                keep = keep and length <= self.max_length

            results.append(keep)
        return results
