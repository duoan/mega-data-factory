"""
Parquet Data Writer

Writes data to Parquet files with incremental writes using PyArrow and fsspec.
Supports both local filesystem and cloud storage (S3, GCS, etc.).
"""

import uuid
from datetime import datetime
from typing import Any

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

from webscale_multimodal_datapipeline.framework import DataWriter


class ParquetDataWriter(DataWriter):
    """DataWriter that writes to Parquet files using PyArrow and fsspec.

    Supports both local filesystem and cloud storage (S3, GCS, etc.).
    """

    def __init__(self, output_path: str, table_name: str = "default"):
        """Initialize Parquet writer.

        Args:
            output_path: Directory path for output files (local or cloud, e.g., 's3://bucket/path')
            table_name: Name of the table/subdirectory
        """
        self.output_path = output_path.rstrip("/")
        self.table_name = table_name

        # Get filesystem from path (auto-detects local, s3://, gs://, etc.)
        self.fs, self.root_path = fsspec.core.url_to_fs(self.output_path)

        # Full output directory including table name
        self.full_output_path = f"{self.root_path}/{self.table_name}"

        # Ensure output directory exists
        self.fs.makedirs(self.full_output_path, exist_ok=True)

    def write(self, data: list[dict[str, Any]]):
        """Write data to Parquet files using PyArrow (fast, no pandas conversion).

        Args:
            data: List of processed records to write
        """
        if not data:
            return

        # Convert directly to PyArrow Table (no pandas overhead)
        arrow_table = pa.Table.from_pylist(data)

        # Generate unique filename with timestamp and UUID to avoid collisions
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex[:8]
        filename = f"part_{timestamp}_{unique_id}.parquet"
        parquet_path = f"{self.full_output_path}/{filename}"

        # Write with compression and optimized settings using fsspec
        with self.fs.open(parquet_path, "wb") as f:
            pq.write_table(
                arrow_table,
                f,
                compression="snappy",  # Fast compression
                row_group_size=50000,  # Larger row groups for better performance
                use_dictionary=True,  # Dictionary encoding for better compression
            )

    def close(self):
        """Close writer (no-op for Parquet)."""
        pass
