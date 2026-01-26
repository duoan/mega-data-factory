#!/usr/bin/env python3
"""Test CommonCrawlLoader."""

from mega_data_factory.loaders import CommonCrawlLoader

loader = CommonCrawlLoader(crawl_id="CC-MAIN-2024-51")

file_list = loader.get_file_list()
print(f"Total WARC files: {len(file_list)}")
print(f"First: {file_list[0].split('/')[-1]}")

# Test file assignment for 4 workers
num_workers = 4
total = len(file_list)
per_worker = total // num_workers

print(f"\nFile assignment for {num_workers} workers:")
for w in range(num_workers):
    start = w * per_worker
    end = start + per_worker if w < num_workers - 1 else total
    print(f"  Worker {w}: {end - start} files")
