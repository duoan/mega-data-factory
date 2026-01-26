"""Filter operators package - lazy loaded to avoid heavy dependencies."""

from mega_data_factory.framework import OperatorRegistry

from .text_length_filter import TextLengthFilter
from .url_filter import URLFilter

# Register text-only filters (no heavy dependencies)
OperatorRegistry.register("TextLengthFilter", TextLengthFilter)
OperatorRegistry.register("UrlFilter", URLFilter)


def _register_image_filters():
    """Lazy register image filters that depend on PIL."""
    from .image_quality_filter import ImageQualityFilter
    OperatorRegistry.register("ImageQualityFilter", ImageQualityFilter)


# Defer heavy imports
try:
    _register_image_filters()
except ImportError:
    pass  # Skip if PIL not available


__all__ = ["TextLengthFilter", "URLFilter"]
