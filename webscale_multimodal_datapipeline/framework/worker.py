"""
Worker: Executes operators on records

Provides RayWorker for executing operators in batch processing.
"""

import logging
from typing import Any

import ray

from .base import DataWriter
from .operator import CombinedOperator, Operator


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
    ):
        """Initialize Ray worker.

        Args:
            name: Worker name
            operators: List of operators to run
            data_writer: Data writer for writing results locally (optional)
        """
        self.name = name
        self.operators = operators
        self.data_writer = data_writer

        # Set up logging (Ray logs appear in Ray Dashboard)
        self.logger = logging.getLogger(f"RayWorker.{name}")
        self.logger.setLevel(logging.INFO)

        # Initialize batch counters for progress tracking
        self.batch_count = 0
        self.record_count = 0
        self.processed_count = 0

        # Combine operators if multiple
        if len(operators) == 1:
            self.operator = operators[0]
        else:
            self.operator = CombinedOperator(operators)

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

        # Process batch using process_batch (all operators have this method)
        results = self.operator.process_batch(records)

        # Filter out None values (from filters/dedups)
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
            self.logger.info(
                f"Progress: {self.batch_count} batches, "
                f"{self.record_count} records processed, "
                f"{self.processed_count} records passed "
                f"({self.processed_count}/{self.record_count}={100 * self.processed_count / max(1, self.record_count):.1f}% pass rate)"
            )

        return processed if processed else None

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
