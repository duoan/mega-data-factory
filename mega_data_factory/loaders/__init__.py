"""Data Loaders package."""

from mega_data_factory.framework import DataLoaderRegistry

from .commoncrawl_loader import CommonCrawlLoader
from .huggingface_loader import HuggingFaceLoader

DataLoaderRegistry.register("HuggingFaceLoader", HuggingFaceLoader)
DataLoaderRegistry.register("CommonCrawlLoader", CommonCrawlLoader)

__all__ = ["HuggingFaceLoader", "CommonCrawlLoader"]
