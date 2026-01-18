"""
Filter operators package.

This package contains filter implementations.
Filters are automatically registered when this package is imported.
"""

from framework import OperatorRegistry

from .quality_filter import QualityFilter

# Register all filters with the framework
OperatorRegistry.register("QualityFilter", QualityFilter)

__all__ = [
    "QualityFilter",
]
