"""
Worker: Executes operators on records

Provides RayWorker for executing operators in batch processing.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

import ray

from .base import DataWriter
from .operator import CombinedOperator, Operator


@dataclass
class WorkerBatchResult:
    """Result of processing a batch in a worker.

    Attributes:
        passed: Records that passed through all operators
        rejected: Records that were rejected by any operator, with metadata
    """

    passed: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)


@ray.remote
class RayWorker:
    """Ray Actor for distributed batch processing.

    Each RayWorker processes batches of records on a Ray node.
    Workers are created as Ray Actors and process batches remotely.
    """

    def __init__(
        self,
        name: str,
        operators: list[Operator],
        data_writer: DataWriter | None = None,
        rejected_writer: DataWriter | None = None,
        collect_rejected: bool = False,
    ):
        """Initialize Ray worker.

        Args:
            name: Worker name
            operators: List of operators to run
            data_writer: Data writer for writing results locally (optional)
            rejected_writer: Data writer for writing rejected samples (optional)
            collect_rejected: If True, collect rejected samples for deep dive analysis
        """
        self.name = name
        self.operators = operators
        self.data_writer = data_writer
        self.rejected_writer = rejected_writer
        self.collect_rejected = collect_rejected

        # Set up logging (Ray logs appear in Ray Dashboard)
        self.logger = logging.getLogger(f"RayWorker.{name}")
        self.logger.setLevel(logging.INFO)

        # Initialize batch counters for progress tracking
        self.batch_count = 0
        self.record_count = 0
        self.processed_count = 0
        self.rejected_count = 0

        # Combine operators if multiple
        if len(operators) == 1:
            self.operator = operators[0]
        else:
            self.operator = CombinedOperator(operators, collect_rejected=collect_rejected)

    def process_batch_with_records(self, records_or_ref, should_write: bool = False) -> list[dict[str, Any]] | None:
        """Process a batch of records and optionally write to storage.

        Used for multi-stage pipeline where only the last stage writes data.
        Supports both direct records and Ray ObjectRef for chaining.

        Args:
            records_or_ref: List of input records or Ray ObjectRef to records
            should_write: If True, write results to storage. Only last stage should set this to True.

        Returns:
            List of processed records (None values filtered out), or None if all filtered
        """
        self.logger.info(
            f"process_batch_with_records called (should_write={should_write}, type={type(records_or_ref)})"
        )

        # Handle Ray ObjectRef (for chaining) - Ray will automatically resolve it
        if isinstance(records_or_ref, ray.ObjectRef):
            self.logger.info("Resolving ObjectRef...")
            records = ray.get(records_or_ref)
            self.logger.info(f"ObjectRef resolved, got {len(records) if records else 0} records")
        else:
            records = records_or_ref

        # Handle None/empty records (upstream may have filtered everything)
        if not records:
            self.logger.info("Empty records, returning []")
            return []

        # Process batch - use rejected collection if enabled
        if self.collect_rejected:
            batch_result = self.operator.process_batch_with_rejected(records)
            processed = batch_result.passed
            rejected = batch_result.rejected

            # Write rejected samples immediately (not just on last stage)
            # Each stage writes its own rejected samples for deep dive analysis
            if self.rejected_writer and rejected:
                self.rejected_writer.write(rejected)

            self.rejected_count += len(rejected)
        else:
            # Standard processing without rejected collection
            results = self.operator.process_batch(records)
            processed = [r for r in results if r is not None]

        # Update counters
        self.batch_count += 1
        self.record_count += len(records)
        self.processed_count += len(processed)

        # Only write if this is the last stage (better I/O efficiency)
        if should_write and self.data_writer and processed:
            self.data_writer.write(processed)

        # Log progress every 10 batches or on first batch (visible in Ray Dashboard)
        if self.batch_count % 10 == 1 or self.batch_count == 1:
            rejected_info = f", {self.rejected_count} rejected" if self.collect_rejected else ""
            self.logger.info(
                f"Progress: {self.batch_count} batches, "
                f"{self.record_count} records processed, "
                f"{self.processed_count} records passed{rejected_info} "
                f"({self.processed_count}/{self.record_count}={100 * self.processed_count / max(1, self.record_count):.1f}% pass rate)"
            )

        return processed if processed else None

    def process_batch_with_rejected_records(self, records_or_ref, should_write: bool = False) -> WorkerBatchResult:
        """Process a batch and return both passed and rejected records.

        This method explicitly returns rejected samples for collection by the executor.
        Used when collect_rejected is enabled for deep dive analysis.

        Args:
            records_or_ref: List of input records or Ray ObjectRef to records
            should_write: If True, write results to storage.

        Returns:
            WorkerBatchResult containing passed and rejected records
        """
        self.logger.info(f"process_batch_with_rejected_records called (should_write={should_write})")

        # Handle Ray ObjectRef (for chaining) - Ray will automatically resolve it
        if isinstance(records_or_ref, ray.ObjectRef):
            self.logger.info("Resolving ObjectRef...")
            records = ray.get(records_or_ref)
            self.logger.info(f"ObjectRef resolved, got {len(records) if records else 0} records")
        else:
            records = records_or_ref

        # Handle None/empty records
        if not records:
            self.logger.info("Empty records, returning empty result")
            return WorkerBatchResult(passed=[], rejected=[])

        # Process batch with rejected collection
        batch_result = self.operator.process_batch_with_rejected(records)
        processed = batch_result.passed
        rejected = batch_result.rejected

        # Update counters
        self.batch_count += 1
        self.record_count += len(records)
        self.processed_count += len(processed)
        self.rejected_count += len(rejected)

        # Write passed records if this is the last stage
        if should_write and self.data_writer and processed:
            self.data_writer.write(processed)

        # Write rejected samples if writer is configured
        if should_write and self.rejected_writer and rejected:
            self.rejected_writer.write(rejected)

        # Log progress
        if self.batch_count % 10 == 1 or self.batch_count == 1:
            self.logger.info(
                f"Progress: {self.batch_count} batches, "
                f"{self.record_count} records processed, "
                f"{self.processed_count} passed, {self.rejected_count} rejected "
                f"({self.processed_count}/{self.record_count}={100 * self.processed_count / max(1, self.record_count):.1f}% pass rate)"
            )

        return WorkerBatchResult(passed=processed, rejected=rejected)

    def get_operator_stats(self) -> dict[str, Any]:
        """Get performance statistics from all operators in this worker.

        Returns:
            Dictionary mapping operator class names to their statistics
        """
        stats = {}
        # Check if CombinedOperator (has operators attribute)
        if hasattr(self.operator, "operators"):
            # CombinedOperator - get stats from individual operators
            for op in self.operator.operators:
                op_name = op.__class__.__name__
                stats[op_name] = op.get_stats()
        else:
            # Single operator
            op_name = self.operator.__class__.__name__
            stats[op_name] = self.operator.get_stats()
        return stats

    def get_rejected_count(self) -> int:
        """Get total count of rejected records."""
        return self.rejected_count
