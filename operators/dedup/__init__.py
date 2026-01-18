"""
Deduplication operators package.

This package contains deduplication implementations.
Dedups are automatically registered when this package is imported.
"""

from framework import OperatorRegistry

from .phash_dedup import PhashDeduplicator

# Register all dedup operators with the framework
OperatorRegistry.register("PhashDeduplicator", PhashDeduplicator)

__all__ = [
    "PhashDeduplicator",
]
