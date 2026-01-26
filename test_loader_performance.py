#!/usr/bin/env python3
"""Test DataLoaderWorker performance."""

import time
from mega_data_factory.loaders import HuggingFaceLoader
from mega_data_factory.framework.loader_worker import DataLoaderWorker
import ray

ray.init(ignore_reinit_error=True)

loader = HuggingFaceLoader(
    dataset_name="jp1924/Laion400m-1",
    split="train",
    streaming=True,
)

print("Creating DataLoaderWorker...")
worker = DataLoaderWorker.remote(
    data_loader=loader,
    shard_id=0,
    num_shards=4,
    batch_size=100,
    checkpoint_interval=500,
    iterator_refresh_interval=5,
)

print("\nFetching 10 batches...\n")

start_time = time.time()
for i in range(10):
    batch_start = time.time()
    result = ray.get(worker.get_next_batch.remote(max_records=2000))
    batch_time = time.time() - batch_start

    if result["batch"]:
        print(f"Batch {i+1}: {len(result['batch'])} records, time: {batch_time:.2f}s")
    else:
        print(f"Batch {i+1}: Completed")
        break

print(f"\nTotal time: {time.time() - start_time:.2f}s")
ray.shutdown()
