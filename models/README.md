# Models Package

This package contains model training and inference code for the Z-Image data pipeline.

## Structure

```
models/
├── kmeans/              # KMeans clustering for semantic deduplication
│   ├── trainer.py       # Model training
│   └── inference.py     # Model inference
├── classifier/          # Classification models
│   └── trainer.py       # Base classifier trainer
├── quality_assessment/  # Quality assessment models
│   ├── trainer.py       # Model training
│   └── inference.py     # Model inference
├── train_kmeans.py     # Training script for KMeans
└── train_quality_assessment.py  # Training script for quality model
```

## KMeans Clustering

Used for semantic deduplication by clustering similar images.

### Distributed Training (Recommended)

Train distributed KMeans using Ray with parquet files as input:

```bash
python models/kmeans/distributed_train.py \
    --data_urls ./data/features_*.parquet \
    --n_clusters 100 \
    --n_workers 4 \
    --output_dir ./kmeans_training
```

**Input Format:**
- Parquet files must have two columns:
  - `id` (string): Unique identifier for each record
  - `feature` (array/embedding): Feature vector for clustering

**Output:**
- Intermediate files: `iter_{iteration}_shard_{worker_id}_assignments.npy`
- Centroids: `iter_{iteration}/centroids.npy`
- Final model: Saved to specified `--model_path`

### Local Training

For small datasets, use the local trainer:

```bash
python models/train_kmeans.py \
    --features_path features.npy \
    --n_clusters 100 \
    --output_path ./models/kmeans/kmeans_model.pkl
```

### Usage in Pipeline

The cluster IDs can be used as bucket IDs for semantic deduplication:

```python
from models.kmeans.inference import KMeansInference

# Load model
inference = KMeansInference(model_path="./models/kmeans/kmeans_model.pkl")

# Get cluster ID (bucket ID)
cluster_id = inference.predict_cluster(image_feature)
```

## Quality Assessment Model

Neural network model for assessing visual degradations.

### Training

```bash
python models/train_quality_assessment.py \
    --train_images train_images.npy \
    --train_labels train_labels.npy \
    --output_path ./models/quality_assessment/quality_model.pth \
    --epochs 10 \
    --batch_size 32 \
    --device cuda
```

### Usage in Pipeline

```python
from models.quality_assessment.inference import QualityAssessmentInference

# Load model
inference = QualityAssessmentInference(
    model_path="./models/quality_assessment/quality_model.pth",
    device="cuda"
)

# Predict quality score
score = inference.predict_from_bytes(image_bytes)
```

## Integration with Pipeline

These models can be integrated into the pipeline operators:

1. **KMeans**: Used in `SemanticDeduplicator` to assign cluster IDs
2. **Quality Assessment**: Used in `VisualDegradationsRefiner` to assess image quality
