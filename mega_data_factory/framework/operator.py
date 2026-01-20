"""
Operator: Abstract interface for record processing

Defines the Operator, Refiner, Filter, and Deduplicator base classes.
"""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from .backend import DedupBackend


@dataclass
class BatchResult:
    """Result of processing a batch, including passed and rejected records.

    Attributes:
        passed: Records that passed through the operator
        rejected: Records that were rejected/filtered out, with rejection metadata
    """

    passed: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)


class Operator(ABC):
    """Base class for all operators (batch processing with built-in stats)."""

    def __init__(self, collect_rejected: bool = False):
        """Initialize operator.

        Args:
            collect_rejected: If True, collect rejected samples for deep dive analysis
        """
        self._stats = {
            "input_records": 0,
            "output_records": 0,
            "total_time": 0.0,
            "min_latency": float("inf"),
            "max_latency": 0.0,
            "_latencies": [],
        }
        self._collect_rejected = collect_rejected
        self._rejected_buffer: list[dict[str, Any]] = []

    def process_batch(self, records: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
        """Process a batch of records with automatic stats collection."""
        start_time = time.perf_counter()
        results = self._process_batch_impl(records)
        batch_latency = time.perf_counter() - start_time

        num_input = len(records)
        num_output = sum(1 for r in results if r is not None)

        if num_input > 0:
            self._stats["input_records"] += num_input
            self._stats["output_records"] += num_output
            self._stats["total_time"] += batch_latency
            per_record_latency = batch_latency / num_input
            self._stats["min_latency"] = min(self._stats["min_latency"], per_record_latency)
            self._stats["max_latency"] = max(self._stats["max_latency"], per_record_latency)
            self._stats["_latencies"].extend([per_record_latency] * num_input)
            if len(self._stats["_latencies"]) > 10000:
                self._stats["_latencies"] = self._stats["_latencies"][-10000:]

        return results

    def process_batch_with_rejected(self, records: list[dict[str, Any]]) -> BatchResult:
        """Process a batch and return both passed and rejected records.

        This method is used when collect_rejected is enabled to capture
        rejected samples for deep dive analysis.

        Args:
            records: Input records to process

        Returns:
            BatchResult containing passed and rejected records
        """
        start_time = time.perf_counter()
        batch_result = self._process_batch_with_rejected_impl(records)
        batch_latency = time.perf_counter() - start_time

        num_input = len(records)
        num_output = len(batch_result.passed)

        if num_input > 0:
            self._stats["input_records"] += num_input
            self._stats["output_records"] += num_output
            self._stats["total_time"] += batch_latency
            per_record_latency = batch_latency / num_input
            self._stats["min_latency"] = min(self._stats["min_latency"], per_record_latency)
            self._stats["max_latency"] = max(self._stats["max_latency"], per_record_latency)
            self._stats["_latencies"].extend([per_record_latency] * num_input)
            if len(self._stats["_latencies"]) > 10000:
                self._stats["_latencies"] = self._stats["_latencies"][-10000:]

        return batch_result

    def _process_batch_with_rejected_impl(self, records: list[dict[str, Any]]) -> BatchResult:
        """Internal implementation for processing with rejected collection.

        Default implementation calls _process_batch_impl and doesn't collect rejected.
        Subclasses (Filter, Deduplicator) override this to collect rejected samples.
        """
        results = self._process_batch_impl(records)
        passed = [r for r in results if r is not None]
        return BatchResult(passed=passed, rejected=[])

    @abstractmethod
    def _process_batch_impl(self, records: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
        """Internal batch processing implementation (subclasses implement this)."""
        pass

    def get_stats(self) -> dict[str, Any]:
        """Get performance statistics for this operator.

        Returns:
            Dictionary with performance metrics:
            - input_records: Total number of input records
            - output_records: Total number of output records (after filtering)
            - pass_rate: Percentage of records that passed through
            - total_time: Total processing time (seconds)
            - avg_latency: Average latency per record (seconds)
            - min_latency: Minimum latency (seconds)
            - max_latency: Maximum latency (seconds)
            - p50_latency: 50th percentile latency (seconds)
            - p95_latency: 95th percentile latency (seconds)
            - p99_latency: 99th percentile latency (seconds)
            - throughput: Records per second
        """
        stats = self._stats.copy()
        input_records = stats["input_records"]
        output_records = stats["output_records"]

        if input_records == 0:
            return {
                "input_records": 0,
                "output_records": 0,
                "pass_rate": 0.0,
                "total_time": 0.0,
                "avg_latency": 0.0,
                "min_latency": 0.0,
                "max_latency": 0.0,
                "p50_latency": 0.0,
                "p95_latency": 0.0,
                "p99_latency": 0.0,
                "throughput": 0.0,
            }

        pass_rate = 100.0 * output_records / input_records
        avg_latency = stats["total_time"] / input_records
        min_latency = stats["min_latency"] if stats["min_latency"] != float("inf") else 0.0
        max_latency = stats["max_latency"]

        # Calculate percentiles
        latencies = stats["_latencies"]
        if latencies:
            sorted_latencies = sorted(latencies)
            p50 = sorted_latencies[int(len(sorted_latencies) * 0.50)]
            p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
            p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)]
        else:
            p50 = p95 = p99 = 0.0

        throughput = input_records / stats["total_time"] if stats["total_time"] > 0 else 0.0

        return {
            "input_records": input_records,
            "output_records": output_records,
            "pass_rate": pass_rate,
            "total_time": stats["total_time"],
            "avg_latency": avg_latency,
            "min_latency": min_latency,
            "max_latency": max_latency,
            "p50_latency": p50,
            "p95_latency": p95,
            "p99_latency": p99,
            "throughput": throughput,
        }

    def reset_stats(self):
        """Reset performance statistics."""
        self._stats = {
            "input_records": 0,
            "output_records": 0,
            "total_time": 0.0,
            "min_latency": float("inf"),
            "max_latency": 0.0,
            "_latencies": [],
        }

    def get_output_schema(self) -> dict[str, pa.DataType]:
        """Return the output schema for this operator.

        Returns:
            Dictionary mapping field names to Arrow data types
        """
        return {}


class Refiner(Operator):
    """Refiner operators enrich records by adding new information (inplace)."""

    @abstractmethod
    def refine_batch(self, records: list[dict[str, Any]]) -> None:
        """Refine a batch of records inplace."""
        pass

    def _process_batch_impl(self, records: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
        if not records:
            return []
        self.refine_batch(records)
        return list(records)  # Return a copy to satisfy type checker

    @abstractmethod
    def get_output_schema(self) -> dict[str, pa.DataType]:
        """Return output schema for new fields added by this refiner."""
        pass


class Filter(Operator):
    """Filter operators determine whether records should be kept or filtered out."""

    @abstractmethod
    def should_keep_batch(self, records: list[dict[str, Any]]) -> list[bool]:
        """Determine which records should be kept (True = keep, False = filter)."""
        pass

    def _process_batch_impl(self, records: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
        if not records:
            return []
        keep_flags = self.should_keep_batch(records)
        return [record if keep else None for record, keep in zip(records, keep_flags, strict=False)]

    def _process_batch_with_rejected_impl(self, records: list[dict[str, Any]]) -> BatchResult:
        """Process batch and collect rejected (filtered out) samples.

        Rejected samples are annotated with rejection metadata for deep dive analysis.
        All rejection details are stored in a single _rejection_details JSON field for extensibility.
        """
        if not records:
            return BatchResult(passed=[], rejected=[])

        keep_flags = self.should_keep_batch(records)
        passed = []
        rejected = []

        for record, keep in zip(records, keep_flags, strict=False):
            if keep:
                passed.append(record)
            else:
                # Add rejection metadata for deep dive analysis
                rejected_record = record.copy()
                rejected_record["_rejection_details"] = {
                    "reason": "filtered",
                    "operator": self.__class__.__name__,
                }
                rejected.append(rejected_record)

        return BatchResult(passed=passed, rejected=rejected)

    def get_output_schema(self) -> dict[str, pa.DataType]:
        return {}


class Deduplicator(Operator):
    """Deduplicator operators remove duplicate records.

    Deduplicators use a configurable backend to track seen records and filter duplicates.
    Examples:
    - Perceptual hash deduplication (PhashDeduplicator)
    - Exact match deduplication
    - Semantic deduplication (cluster-based, bucket_id = cluster_id)

    For semantic deduplication, the dedup_key can encode cluster_id information,
    and a custom bucket_id_getter can extract it for routing.
    """

    def __init__(self, backend: DedupBackend | None = None, representative_id_field: str = "id"):
        """Initialize deduplication operator.

        Args:
            backend: Deduplication backend (should be provided by Executor, can be None initially)
            representative_id_field: Field name to use as representative sample identifier (default: "id")
        """
        super().__init__()
        self.backend = backend
        self.representative_id_field = representative_id_field

    @abstractmethod
    def get_dedup_keys_batch(self, records: list[dict[str, Any]]) -> list[str]:
        """Extract deduplication keys from a batch of records."""
        pass

    def get_representative_ids_batch(self, records: list[dict[str, Any]]) -> list[str]:
        """Extract representative sample IDs from a batch of records.

        Override this method to customize how representative IDs are extracted.
        Default implementation uses the field specified by representative_id_field.
        """
        return [str(record.get(self.representative_id_field, "")) for record in records]

    def _process_batch_impl(self, records: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
        if self.backend is None:
            raise RuntimeError("Deduplicator backend not set.")
        if not records:
            return []
        keys = self.get_dedup_keys_batch(records)
        is_new = self.backend.batch_mark_seen(keys)
        return [record if new else None for record, new in zip(records, is_new, strict=False)]

    def _process_batch_with_rejected_impl(self, records: list[dict[str, Any]]) -> BatchResult:
        """Process batch and collect rejected (deduplicated) samples.

        Rejected samples are annotated with dedup key, representative sample ID, and rejection metadata.
        All rejection details are stored in a single _rejection_details JSON field for extensibility.
        """
        if self.backend is None:
            raise RuntimeError("Deduplicator backend not set.")
        if not records:
            return BatchResult(passed=[], rejected=[])

        keys = self.get_dedup_keys_batch(records)
        representative_ids = self.get_representative_ids_batch(records)

        # Check if backend supports tracking representative IDs
        if self.backend.track_representative:
            # Use the new method that returns representative IDs for duplicates
            results = self.backend.batch_mark_seen_with_ids(keys, representative_ids)

            passed = []
            rejected = []

            for record, key, (is_new, rep_id) in zip(records, keys, results, strict=False):
                if is_new:
                    passed.append(record)
                else:
                    # Add rejection metadata for deep dive analysis
                    rejected_record = record.copy()
                    rejection_details: dict[str, Any] = {
                        "reason": "duplicate",
                        "operator": self.__class__.__name__,
                        "dedup_key": key,
                    }
                    if rep_id is not None:
                        rejection_details["representative_id"] = rep_id
                    rejected_record["_rejection_details"] = rejection_details
                    rejected.append(rejected_record)
        else:
            # Fallback to simple method without representative tracking
            is_new_list = self.backend.batch_mark_seen(keys)

            passed = []
            rejected = []

            for record, key, is_new in zip(records, keys, is_new_list, strict=False):
                if is_new:
                    passed.append(record)
                else:
                    # Add rejection metadata for deep dive analysis
                    rejected_record = record.copy()
                    rejected_record["_rejection_details"] = {
                        "reason": "duplicate",
                        "operator": self.__class__.__name__,
                        "dedup_key": key,
                    }
                    rejected.append(rejected_record)

        return BatchResult(passed=passed, rejected=rejected)

    def get_output_schema(self) -> dict[str, pa.DataType]:
        return {}

    def reset(self):
        """Reset deduplication state."""
        if self.backend is not None:
            self.backend.reset()


class CombinedOperator(Operator):
    """Combines multiple operators into one (batch processing only)."""

    def __init__(self, operators: list[Operator], collect_rejected: bool = False):
        """Initialize combined operator.

        Args:
            operators: List of operators to combine
            collect_rejected: If True, collect rejected samples from all operators
        """
        super().__init__(collect_rejected=collect_rejected)
        self.operators = operators

    def _process_batch_impl(self, records: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
        if not records:
            return []

        current_batch: list[dict[str, Any]] = list(records)
        for operator in self.operators:
            results = operator.process_batch(current_batch)
            current_batch = [r for r in results if r is not None]
            if not current_batch:
                break

        return list(current_batch)  # Return a copy to satisfy type checker

    def _process_batch_with_rejected_impl(self, records: list[dict[str, Any]]) -> BatchResult:
        """Process batch through all operators and collect all rejected samples.

        Rejected samples from each operator are collected and aggregated.
        """
        if not records:
            return BatchResult(passed=[], rejected=[])

        current_batch: list[dict[str, Any]] = list(records)
        all_rejected: list[dict[str, Any]] = []

        for operator in self.operators:
            batch_result = operator.process_batch_with_rejected(current_batch)
            current_batch = batch_result.passed
            all_rejected.extend(batch_result.rejected)

            if not current_batch:
                break

        return BatchResult(passed=current_batch, rejected=all_rejected)

    def get_stats(self) -> dict[str, Any]:
        """Get performance statistics from all operators."""
        return {op.__class__.__name__: op.get_stats() for op in self.operators}

    def get_output_schema(self) -> dict[str, pa.DataType]:
        combined = {}
        for op in self.operators:
            if isinstance(op, Refiner):
                combined.update(op.get_output_schema())
        return combined
