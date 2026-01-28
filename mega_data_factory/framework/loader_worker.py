"""
DataLoaderWorker: Ray Actor for distributed data loading

Enables parallel data loading across multiple workers, with each worker
loading a shard of the dataset and producing batches.
"""

import time
from typing import Any

import ray

from .base import DataLoader


@ray.remote
class DataLoaderWorker:
    """Ray Actor for distributed data loading.

    Each worker loads a disjoint shard of the dataset and produces batches
    for downstream processing stages.
    """

    def __init__(
        self,
        data_loader: DataLoader,
        shard_id: int,
        num_shards: int,
        batch_size: int,
        checkpoint_interval: int = 1000,
        assigned_files: list[str] | None = None,
        max_records: int | None = None,
        **kwargs,  # Accept extra args for compatibility
    ):
        """Initialize data loader worker.

        Args:
            data_loader: DataLoader instance
            shard_id: This worker's shard ID (0 to num_shards-1)
            num_shards: Total number of shards
            batch_size: Number of records per batch
            checkpoint_interval: Save checkpoint every N records
            assigned_files: List of files assigned to this worker
            max_records: Maximum records to load (None = unlimited)
        """
        self.data_loader = data_loader
        self.shard_id = shard_id
        self.num_shards = num_shards
        self.batch_size = batch_size
        self.checkpoint_interval = checkpoint_interval
        self.assigned_files = assigned_files
        self.max_records = max_records

        if assigned_files is None:
            raise ValueError("assigned_files is required for file-based loading")

        self.records_processed = 0
        self.checkpoint = None

        # Timing stats for throughput calculation
        self.start_time: float | None = None
        self.end_time: float | None = None
        self.total_load_time: float = 0.0
        self.batches_produced = 0

        # Initialize the data stream once
        self._data_stream = None
        self._initialize_stream()

    def _initialize_stream(self):
        """Initialize the data stream iterator."""
        print(f"[DataLoaderWorker {self.shard_id}] Initializing data stream")

        if not hasattr(self.data_loader, "load_files"):
            raise ValueError(f"Loader {type(self.data_loader).__name__} does not support load_files()")

        self._data_stream = self.data_loader.load_files(
            file_list=self.assigned_files,
            worker_id=self.shard_id,
            checkpoint=self.checkpoint,
        )

    def get_next_batch(
        self,
        max_records: int | None = None,
        **kwargs,
    ) -> dict[str, Any] | None:
        """Get the next batch from this shard (streaming mode).

        IMPORTANT: Returns batch data as ObjectRef to avoid pulling data back to driver.
        The batch_ref can be passed directly to downstream stages for zero-copy processing.

        Args:
            max_records: Optional override for maximum records (None = use self.max_records)
            **kwargs: Additional parameters (not used in streaming mode)

        Returns:
            Dictionary with:
                - 'batch_ref': ObjectRef to batch data in Ray object store (or None if completed)
                - 'batch_size': Number of records in the batch (for tracking without fetching data)
                - 'records_processed': Total records processed so far
                - 'completed': Boolean indicating if loading is complete
        """
        # Start timing on first batch
        if self.start_time is None:
            self.start_time = time.time()

        batch_start = time.time()

        # Use instance max_records if not overridden
        effective_max_records = max_records if max_records is not None else self.max_records

        # Check if we've reached max_records
        if effective_max_records and self.records_processed >= effective_max_records:
            self.end_time = time.time()
            return {
                "batch_ref": None,
                "batch_size": 0,
                "records_processed": self.records_processed,
                "completed": True,
            }

        batch = []

        try:
            for record in self._data_stream:
                batch.append(record)
                self.records_processed += 1

                # Return batch when full
                if len(batch) >= self.batch_size:
                    # Update checkpoint
                    self.checkpoint = self.data_loader.create_checkpoint(
                        shard_id=self.shard_id,
                        records_processed=self.records_processed,
                    )

                    # Save checkpoint periodically
                    if self.records_processed % self.checkpoint_interval == 0:
                        self._save_checkpoint()

                    # Track timing
                    self.total_load_time += time.time() - batch_start
                    self.batches_produced += 1

                    # Put batch into object store and return ref (zero-copy to downstream)
                    batch_ref = ray.put(batch)
                    return {
                        "batch_ref": batch_ref,
                        "batch_size": len(batch),
                        "records_processed": self.records_processed,
                        "completed": False,
                    }

                # Check max_records limit
                if effective_max_records and self.records_processed >= effective_max_records:
                    self.checkpoint = self.data_loader.create_checkpoint(
                        shard_id=self.shard_id,
                        records_processed=self.records_processed,
                    )
                    self._save_checkpoint()

                    self.total_load_time += time.time() - batch_start
                    self.batches_produced += 1
                    self.end_time = time.time()

                    if batch:
                        batch_ref = ray.put(batch)
                        return {
                            "batch_ref": batch_ref,
                            "batch_size": len(batch),
                            "records_processed": self.records_processed,
                            "completed": True,
                        }
                    return {
                        "batch_ref": None,
                        "batch_size": 0,
                        "records_processed": self.records_processed,
                        "completed": True,
                    }

            # Iterator exhausted - return final partial batch if any
            self.checkpoint = self.data_loader.create_checkpoint(
                shard_id=self.shard_id,
                records_processed=self.records_processed,
            )
            self._save_checkpoint()

            self.total_load_time += time.time() - batch_start
            if batch:
                self.batches_produced += 1
            self.end_time = time.time()

            if batch:
                batch_ref = ray.put(batch)
                return {
                    "batch_ref": batch_ref,
                    "batch_size": len(batch),
                    "records_processed": self.records_processed,
                    "completed": True,
                }
            return {
                "batch_ref": None,
                "batch_size": 0,
                "records_processed": self.records_processed,
                "completed": True,
            }

        except StopIteration:
            self.checkpoint = self.data_loader.create_checkpoint(
                shard_id=self.shard_id,
                records_processed=self.records_processed,
            )
            self._save_checkpoint()

            self.total_load_time += time.time() - batch_start
            if batch:
                self.batches_produced += 1
            self.end_time = time.time()

            if batch:
                batch_ref = ray.put(batch)
                return {
                    "batch_ref": batch_ref,
                    "batch_size": len(batch),
                    "records_processed": self.records_processed,
                    "completed": True,
                }
            return {
                "batch_ref": None,
                "batch_size": 0,
                "records_processed": self.records_processed,
                "completed": True,
            }

    def _save_checkpoint(self):
        """Save checkpoint for resume support."""
        self.checkpoint = self.data_loader.create_checkpoint(
            shard_id=self.shard_id,
            records_processed=self.records_processed,
        )
        print(f"[DataLoaderWorker {self.shard_id}] Checkpoint: {self.records_processed} records")

    def get_checkpoint(self) -> dict[str, Any]:
        """Get current checkpoint data."""
        return self.checkpoint or {}

    def restore_checkpoint(self, checkpoint: dict[str, Any]):
        """Restore from checkpoint."""
        self.checkpoint = checkpoint
        self.records_processed = checkpoint.get("records_processed", 0)
        print(f"[DataLoaderWorker {self.shard_id}] Restored checkpoint: {self.records_processed} records")

    def get_stats(self) -> dict[str, Any]:
        """Get worker statistics including throughput."""
        total_time = 0.0
        if self.start_time is not None:
            end = self.end_time if self.end_time else time.time()
            total_time = end - self.start_time

        throughput = self.records_processed / total_time if total_time > 0 else 0.0

        return {
            "shard_id": self.shard_id,
            "num_shards": self.num_shards,
            "records_processed": self.records_processed,
            "batches_produced": self.batches_produced,
            "total_time_sec": total_time,
            "load_time_sec": self.total_load_time,
            "throughput_records_per_sec": throughput,
            "has_checkpoint": self.checkpoint is not None,
        }
