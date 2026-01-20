"""
Pipeline Framework: Configuration-Driven Distributed Processing Framework

This package provides a flexible framework for building data processing pipelines.
All public APIs are exported from this module.
"""

# Config classes
# Backend classes
from .backend import (
    DedupBackend,
)

# Base classes
from .base import (
    DataLoader,
    DataWriter,
)
from .config import (
    DataLoaderConfig,
    DataWriterConfig,
    ExecutorConfig,
    OperatorConfig,
    PipelineConfig,
    RejectedSamplesConfig,
    StageConfig,
    StageWorkerConfig,
)

# Executor
from .executor import (
    Executor,
)

# Operator classes
from .operator import (
    BatchResult,
    CombinedOperator,
    Deduplicator,
    Filter,
    Operator,
    Refiner,
)

# Registry classes
from .registry import (
    DataLoaderRegistry,
    DataWriterRegistry,
    OperatorRegistry,
)

# Worker classes
from .worker import RayWorker, WorkerBatchResult

# Export all public APIs
__all__ = [
    # Config
    "OperatorConfig",
    "StageWorkerConfig",
    "StageConfig",
    "DataLoaderConfig",
    "DataWriterConfig",
    "ExecutorConfig",
    "RejectedSamplesConfig",
    "PipelineConfig",
    # Operator
    "Operator",
    "Refiner",
    "Filter",
    "Deduplicator",
    "CombinedOperator",
    "BatchResult",
    # Backend
    "DedupBackend",
    # Registry
    "OperatorRegistry",
    "DataLoaderRegistry",
    "DataWriterRegistry",
    # Base
    "DataLoader",
    "DataWriter",
    # Worker
    "RayWorker",
    "WorkerBatchResult",
    # Executor
    "Executor",
]
