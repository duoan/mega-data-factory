"""CommonCrawl WARC DataLoader with Rust-accelerated text extraction."""

from __future__ import annotations

import gzip
import os
import time
from collections.abc import Iterator
from typing import Any

import requests

from mega_data_factory.framework import DataLoader


class CommonCrawlLoader(DataLoader):
    """Streaming WARC loader with Rust text extraction."""

    def __init__(
        self,
        crawl_id: str,
        base_url: str = "https://data.commoncrawl.org/",
        cache_dir: str | None = None,
        num_files: int | None = None,
    ):
        self.crawl_id = crawl_id
        self.base_url = base_url.rstrip("/") + "/"
        self.cache_dir = cache_dir or os.path.expanduser("~/.cache/commoncrawl")
        self.num_files = num_files
        self._file_list: list[str] | None = None

    def get_file_list(self, max_samples: int | None = None, num_workers: int = 1) -> list[str]:
        """Get list of WARC file paths."""
        if self._file_list is not None:
            return self._file_list

        # Calculate files needed: ~5K records per file
        num_files = self.num_files
        if num_files is None and max_samples:
            num_files = max(num_workers, max_samples // 5000 + 1)

        url = f"{self.base_url}crawl-data/{self.crawl_id}/warc.paths.gz"
        paths = []

        r = requests.get(url, stream=True, timeout=300)
        r.raise_for_status()
        r.raw.decode_content = False

        for line in gzip.GzipFile(fileobj=r.raw):
            path = line.decode("utf-8", errors="ignore").strip()
            if path:
                paths.append(path)
                if num_files and len(paths) >= num_files:
                    break

        self._file_list = paths
        print(f"[CommonCrawl] {len(paths)} WARC files")
        return paths

    def load_files(
        self,
        file_list: list[str],
        worker_id: int | None = None,
        checkpoint: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Load WARC files and yield records with extracted text.

        Uses Rust-accelerated WARC parsing for 5-10x faster processing:
        - flate2 for gzip decompression (faster than Python gzip)
        - Native WARC parsing (faster than warcio)
        - Parallel text extraction with rayon
        """
        from mega_data_factory.rust_operators import warc_extract_records

        label = f"W{worker_id}" if worker_id is not None else "L"
        skip = checkpoint.get("records_processed", 0) if checkpoint else 0
        count = 0

        for warc_path in file_list:
            local_path = self._download(warc_path)

            # 🦀 Rust: decompress + parse WARC + extract text in one call
            records = warc_extract_records(local_path)

            for url, warc_date, title, text, text_length in records:
                count += 1
                if count <= skip:
                    continue

                yield {
                    "crawl_id": self.crawl_id,
                    "warc_path": warc_path,
                    "url": url,
                    "warc_date": warc_date,
                    "title": title,
                    "text": text,
                    "text_length": text_length,
                }

        print(f"[{label}] {count} records")

    def _download(self, warc_path: str) -> str:
        """Download WARC to cache."""
        os.makedirs(os.path.join(self.cache_dir, self.crawl_id), exist_ok=True)
        filename = warc_path.rsplit("/", 1)[-1]
        local_path = os.path.join(self.cache_dir, self.crawl_id, filename)

        if os.path.exists(local_path):
            return local_path

        print(f"[DL] {filename}...")
        url = f"{self.base_url}{warc_path}"

        for attempt in range(3):
            try:
                r = requests.get(url, stream=True, timeout=300)
                r.raise_for_status()
                tmp = local_path + ".tmp"
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(131072):
                        f.write(chunk)
                os.rename(tmp, local_path)
                print(f"[DL] {filename} done ({os.path.getsize(local_path) // 1048576}MB)")
                return local_path
            except Exception as e:
                if attempt == 2:
                    raise RuntimeError(f"Download failed: {warc_path}") from e
                time.sleep(2 ** attempt)

        return local_path
