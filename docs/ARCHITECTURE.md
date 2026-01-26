# Mega Data Factory Architecture

A deep dive into the distributed, pipeline-parallel architecture that enables web-scale (100B+) data processing.

## Table of Contents

- [Overview](#overview)
- [Core Design Principles](#core-design-principles)
- [Three-Layer Distributed Architecture](#three-layer-distributed-architecture)
- [Pipeline Parallelism via ObjectRef Chaining](#pipeline-parallelism-via-objectref-chaining)
- [Distributed Data Loading](#distributed-data-loading)
- [Backpressure Control](#backpressure-control)
- [Distributed Deduplication](#distributed-deduplication)
- [Elastic Worker Scaling](#elastic-worker-scaling)
- [Rust Acceleration](#rust-acceleration)
- [Theoretical Scalability](#theoretical-scalability)

## Overview

Mega Data Factory is designed from the ground up to process hundreds of billions of records. The architecture achieves this through:

1. **Pipeline Parallelism**: Multiple stages process different batches concurrently
2. **Data Parallelism**: Multiple workers within each stage process batches in parallel
3. **Distributed State**: Deduplication state is sharded across multiple actors
4. **Backpressure Control**: Memory-safe execution with bounded in-flight batches

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                   Driver                                    │
│  • Orchestrates pipeline execution                                          │
│  • Submits ObjectRef chains (non-blocking)                                  │
│  • Enforces backpressure limits                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
         ┌────────────────────────────┼────────────────────────────┐
         ▼                            ▼                            ▼
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│  LoaderWorker 0 │          │  LoaderWorker 1 │          │  LoaderWorker N │
│  (Shard 0)      │          │  (Shard 1)      │          │  (Shard N)      │
└─────────────────┘          └─────────────────┘          └─────────────────┘
         │                            │                            │
         └────────────────────────────┼────────────────────────────┘
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Ray Object Store (Shared Memory)                    │
│  • Zero-copy data transfer between actors                                   │
│  • Automatic distributed caching                                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
         ┌────────────────────────────┼────────────────────────────┐
         ▼                            ▼                            ▼
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│   CPU Workers   │    →     │   GPU Workers   │    →     │   CPU Workers   │
│   (Stage 1)     │          │   (Stage 2)     │          │   (Stage 3)     │
│   2-8 replicas  │          │   1-2 replicas  │          │   2-4 replicas  │
└─────────────────┘          └─────────────────┘          └─────────────────┘
```

## Core Design Principles

### 1. Non-Blocking Driver

The driver (Executor) never performs computation. It only:
- Constructs the task dependency graph
- Submits tasks to Ray
- Monitors progress and enforces backpressure

This allows the driver to submit thousands of batches per second.

### 2. Data Flows Through Object Store

All data transfers happen through Ray's Object Store:
- Zero-copy reads within the same node
- Automatic spilling to disk when memory is full
- Transparent distribution across nodes

### 3. Stateless Workers, Stateful Backends

Workers are stateless and can be freely scaled. Shared state (like deduplication) lives in dedicated backend actors with bucketing for scalability.

## Pipeline Parallelism via ObjectRef Chaining

### The Problem with Sequential Execution

Traditional batch processing blocks at each stage:

```python
# Sequential execution (BAD)
for batch in loader:
    result = stage1.process(batch)      # Block until complete
    result = stage2.process(result)     # Block until complete
    result = stage3.process(result)     # Block until complete
    writer.write(result)
# Total time = N × (T1 + T2 + T3)
```

### ObjectRef Chaining Solution

Mega Data Factory passes Ray ObjectRefs instead of data:

```python
# Pipeline parallel execution (GOOD)
def _submit_batch_chain(self, records):
    # Put data in Object Store, get a reference
    current_ref = ray.put(records)

    for stage_idx, worker_group in enumerate(self.stages):
        # Round-robin worker selection
        worker = worker_group[self.stage_indices[stage_idx] % len(worker_group)]
        self.stage_indices[stage_idx] += 1

        # KEY: Pass ObjectRef, not data!
        # Ray automatically handles dependencies
        current_ref = worker.process_batch_with_records.remote(
            current_ref,  # Previous stage's output reference
            should_write=(stage_idx == len(self.stages) - 1)
        )

    return current_ref  # Returns immediately!
```

### Execution Timeline

```
Time →
Driver:    [B0] [B1] [B2] [B3] [B4] [B5] [B6] [B7] ...
              ↓    ↓    ↓    ↓    ↓    ↓    ↓    ↓
Stage 1:      [==B0==] [==B1==] [==B2==] [==B3==] ...
                       [==B0==] [==B1==] [==B2==] ...
Stage 2:                        [==B0==] [==B1==] ...
                                         [==B0==] ...
Stage 3:
```

**Key insight**: Driver submission is O(1). While Stage 1 processes B1, Stage 2 processes B0, and Stage 3 processes an earlier batch. All stages run concurrently on different batches.

### Why This Scales

| Approach | Driver Time | Stage Utilization | Memory |
|----------|-------------|-------------------|--------|
| Sequential | O(N × Stages) | 1/Stages | Low |
| ObjectRef Chain | O(N) | ~100% | Controlled |

## Distributed Data Loading

### File-Based Sharding

The Executor scans available files and distributes them to loader workers:

```python
# Executor._create_loader_workers()
all_files = data_loader.get_file_list()  # e.g., 1000 WARC files
files_per_worker = total_files // num_workers

# Worker 0: files[0:62]
# Worker 1: files[62:124]
# ...
# Worker 15: files[938:1000]
```

### Streaming Batch Production

Each `DataLoaderWorker` is a Ray Actor that streams batches:

```python
@ray.remote
class DataLoaderWorker:
    def __init__(self, assigned_files, shard_id, batch_size, ...):
        self.assigned_files = assigned_files  # Disjoint file set
        self._data_stream = None

    def get_next_batch(self):
        """Called repeatedly by Executor to get next batch."""
        batch = []
        for record in self._data_stream:
            batch.append(record)
            if len(batch) >= self.batch_size:
                return {"batch": batch, "completed": False}

        return {"batch": batch or None, "completed": True}
```

### Properties

- **Linear Scaling**: 16 workers → 16× throughput
- **No Shared State**: Each worker loads independently, no locks
- **Checkpoint Support**: Workers track progress for fault recovery
- **Memory Efficient**: Streaming iteration, not loading all data

## Backpressure Control

### The OOM Problem

Without backpressure, fast loaders can overwhelm slow processors:

```
Loader speed: 10,000 records/sec
GPU speed:    1,000 records/sec
→ Object Store grows at 9,000 records/sec
→ OOM in minutes
```

### Solution: Bounded In-Flight Batches

```python
# Executor._execute_distributed()
max_in_flight = config.executor.max_in_flight or len(self.loader_workers)

while active_loaders or batch_pipeline:
    pipeline_has_capacity = len(batch_pipeline) < max_in_flight

    if ready_batch and pipeline_has_capacity:
        # Submit new batch to pipeline
        self._submit_batch_chain(batch)
    elif not pipeline_has_capacity:
        # Pipeline full - wait for completion
        # This is the backpressure mechanism
        for result in self._collect_completed(batch_pipeline, max_in_flight):
            yield result
```

### Memory Characteristics

| Config | In-Flight | Memory Usage |
|--------|-----------|--------------|
| `max_in_flight=8` | ≤8 batches | ~8 × batch_size × record_size |
| `max_in_flight=16` | ≤16 batches | ~16 × batch_size × record_size |

With `batch_size=1000` and ~1KB/record: `max_in_flight=16` uses ~16MB for batch data.

## Distributed Deduplication

### The Single-Actor Bottleneck

A naive deduplication actor becomes a bottleneck:

```python
# BAD: Single actor for 100B keys
@ray.remote
class NaiveDedupBackend:
    def __init__(self):
        self.seen = set()  # 100B keys = ~4TB memory on ONE machine!
```

### Bucketed Deduplication

Mega Data Factory shards deduplication state across multiple actors:

```python
class DedupBackend:
    """
    Performance guidelines:
    - Small datasets (<1B keys): 16-64 buckets
    - Medium datasets (1B-10B keys): 256-1000 buckets
    - Large datasets (10B-100B keys): 1000-10000 buckets
    - Target: ~10M-100M keys per bucket
    """

    def __init__(self, num_buckets=16):
        # Create one actor per bucket
        self.bucket_actors = [
            _DedupBackendBucketActor.remote(i)
            for i in range(num_buckets)
        ]

    def batch_mark_seen(self, keys):
        # 1. Group keys by bucket
        bucket_groups = defaultdict(list)
        for idx, key in enumerate(keys):
            bucket_id = hash(key) % self.num_buckets
            bucket_groups[bucket_id].append((idx, key))

        # 2. Send to bucket actors IN PARALLEL
        futures = {
            bid: self.bucket_actors[bid].batch_mark_seen.remote(keys)
            for bid, keys in bucket_groups.items()
        }

        # 3. Collect results
        results = [None] * len(keys)
        for bid, future in futures.items():
            bucket_results = ray.get(future)
            for (orig_idx, _), result in zip(bucket_groups[bid], bucket_results):
                results[orig_idx] = result

        return results
```

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DedupBackend (Coordinator)                   │
│  • Stateless - only routes requests                             │
│  • hash(key) % num_buckets → bucket actor                       │
└─────────────────────────────────────────────────────────────────┘
                              │
       ┌──────────────────────┼──────────────────────┐
       ▼                      ▼                      ▼
┌─────────────┐        ┌─────────────┐        ┌─────────────┐
│  Bucket 0   │        │  Bucket 1   │        │  Bucket N   │
│  Actor      │        │  Actor      │        │  Actor      │
│             │        │             │        │             │
│ seen: Set   │        │ seen: Set   │        │ seen: Set   │
│ ~100M keys  │        │ ~100M keys  │        │ ~100M keys  │
│ ~4GB RAM    │        │ ~4GB RAM    │        │ ~4GB RAM    │
└─────────────┘        └─────────────┘        └─────────────┘

100B keys / 1000 buckets = 100M keys/bucket
Distributed across cluster → Linear memory scaling
```

## Elastic Worker Scaling

### Configuration

```yaml
stages:
  - name: cpu_stage
    worker:
      min_replicas: 2   # Minimum workers (guaranteed)
      max_replicas: 8   # Maximum workers (if resources available)
      resources:
        cpu: 1

  - name: gpu_stage
    worker:
      min_replicas: 1
      max_replicas: 4
      resources:
        gpu: 0.5  # 2 workers per GPU (model fits in half GPU memory)
```

### Dynamic Allocation

```python
# Executor creates workers up to max_replicas
for replica_id in range(max_replicas):
    try:
        worker = RayWorker.options(
            num_cpus=stage_config.worker.resources.get("cpu", 1),
            num_gpus=stage_config.worker.resources.get("gpu", 0),
        ).remote(name, operators, writer)

        stage_workers.append(worker)

    except Exception as e:
        # Resource unavailable - check if we have minimum
        if len(stage_workers) >= min_replicas:
            break  # Proceed with available workers
        else:
            raise  # Cannot meet minimum requirement
```

### Load Balancing

Round-robin distribution ensures even load:

```python
def _submit_batch_chain(self, records):
    for stage_idx, worker_group in enumerate(self.stages):
        # Round-robin selection
        worker_idx = self.stage_indices[stage_idx] % len(worker_group)
        worker = worker_group[worker_idx]
        self.stage_indices[stage_idx] += 1

        current_ref = worker.process_batch_with_records.remote(current_ref)
```

## Rust Acceleration

### Why Rust?

Python's GIL prevents true parallelism for CPU-bound tasks. Rust extensions bypass this limitation.

### Implementation

```rust
// src/lib.rs
use rayon::prelude::*;

#[pyfunction]
fn image_assess_quality_batch(image_bytes_list: Vec<Vec<u8>>) -> Vec<(f64, f64)> {
    image_bytes_list
        .par_iter()  // Rayon parallel iterator - uses all CPU cores
        .map(|bytes| {
            let img = image::load_from_memory(bytes)?;
            let compression = detect_compression_artifacts(&img);
            let entropy = calculate_entropy(&img);
            (compression, entropy)
        })
        .collect()
}

#[pyfunction]
fn html_extract_text_batch(html_list: Vec<String>) -> Vec<(String, String)> {
    html_list
        .par_iter()  // Parallel HTML parsing
        .map(|html| {
            let mut readability = Readability::new(html);
            let article = readability.parse()?;
            (article.title, article.text_content)
        })
        .collect()
}
```

### Performance Impact

| Operation | Python | Rust + Rayon | Speedup |
|-----------|--------|--------------|---------|
| Image quality assessment | 100 img/sec | 2,500 img/sec | 25× |
| Perceptual hashing | 150 img/sec | 1,500 img/sec | 10× |
| HTML text extraction | 500 doc/sec | 5,000 doc/sec | 10× |

## Real-World Benchmarks

### Text Pipeline (CommonCrawl, 8 CPU)

Actual benchmark processing 1M records:

```
============================================================
Pipeline: CommonCrawl → URLFilter → TextLengthFilter
Records: 1,000,000
Time: 49.11 seconds
============================================================

Operator Throughput:
  URLFilter:          20,362 rec/sec   (98.1% pass rate)
  TextLengthFilter: 1,976,454 rec/sec   (96.4% pass rate)

Stage Throughput:     20,362 rec/sec   (bottleneck: URLFilter regex)
============================================================
```

### Image Pipeline (LAION, M1 Pro)

Actual benchmark processing 1K records with GPU:

```
============================================================
Pipeline: LAION → Quality → Dedup → CLIP Embedding
Records: 1,000
============================================================

CPU Stage (Rust-accelerated):
  ImageMetadataRefiner:         27,000 rec/sec
  ImageTechnicalQualityRefiner:  2,500 rec/sec  🦀
  ImageQualityFilter:        4,200,000 rec/sec
  ImagePhashDeduplicator:        1,500 rec/sec  🦀
  Stage Total:                   1,630 rec/sec

GPU Stage:
  ImageClipEmbeddingRefiner:       132 rec/sec  🖥️
============================================================
```

## Theoretical Scalability

Based on real benchmarks, projected scaling:

### Text Pipeline Scaling

| Cluster Size | Throughput | 1B docs | 100B docs |
|--------------|------------|---------|-----------|
| 8 CPU (measured) | 20K rec/sec | 14 hours | 58 days |
| 64 CPU | 160K rec/sec | 1.7 hours | 7 days |
| 512 CPU | 1.3M rec/sec | 13 min | 21 hours |
| 4096 CPU | 10M rec/sec | 2 min | 2.8 hours |

### Image Pipeline Scaling (with GPU)

| Cluster Size | CPU Stage | GPU Stage | 1B images | 100B images |
|--------------|-----------|-----------|-----------|-------------|
| 8 CPU, 1 GPU | 1.6K/sec | 132/sec | 88 days | 24 years |
| 32 CPU, 4 GPU | 6.5K/sec | 528/sec | 22 days | 6 years |
| 128 CPU, 16 GPU | 26K/sec | 2.1K/sec | 5.5 days | 1.5 years |
| 512 CPU, 64 GPU | 104K/sec | 8.4K/sec | 1.4 days | 138 days |

**Key insight**: Image pipelines are GPU-bound. Text pipelines scale linearly with CPU.

### Image Pipeline (CPU-only, no embeddings)

Without GPU embedding extraction (quality filtering + dedup only):

| Cluster Size | Throughput | 1B images | 100B images |
|--------------|------------|-----------|-------------|
| 8 CPU | 1.6K rec/sec | 7 days | 2 years |
| 64 CPU | 13K rec/sec | 21 hours | 89 days |
| 512 CPU | 104K rec/sec | 2.7 hours | 11 days |
| 4096 CPU | 832K rec/sec | 20 min | 1.4 days |

## Summary

| Design Decision | Problem Solved | Scalability |
|-----------------|----------------|-------------|
| ObjectRef Chaining | Stage blocking | O(1) driver overhead |
| Distributed Loading | Data ingestion bottleneck | O(N) loaders |
| Backpressure Control | Memory explosion | Bounded memory |
| Bucketed Deduplication | Single-actor bottleneck | O(B) buckets |
| Elastic Workers | Resource utilization | Auto-scaling |
| Rust Acceleration | CPU bottleneck | 10-25× speedup |

This architecture enables processing **hundreds of billions of records** - the scale required for training foundation models.
