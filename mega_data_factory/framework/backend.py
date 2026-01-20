"""
DedupBackend: Distributed deduplication state using Ray Actor

Provides deduplication backend with bucketing.
"""

from collections.abc import Callable

import ray
import ray.exceptions


@ray.remote
class _DedupBackendBucketActor:
    """Ray Actor for a single deduplication bucket.

    Each actor maintains only one bucket's seen set.
    This distributes memory across multiple actors.
    """

    def __init__(self, bucket_id: int, track_representative: bool = False):
        self.bucket_id = bucket_id
        self.seen: set = set()
        self.track_representative = track_representative
        # Map from dedup_key to representative sample identifier (e.g., record id)
        self.representative: dict[str, str] = {} if track_representative else {}

    def is_seen(self, key: str) -> bool:
        return key in self.seen

    def mark_seen(self, key: str, representative_id: str | None = None) -> bool:
        if key in self.seen:
            return False
        self.seen.add(key)
        if self.track_representative and representative_id is not None:
            self.representative[key] = representative_id
        return True

    def batch_mark_seen(self, keys: list[str]) -> list[bool]:
        results = []
        for key in keys:
            if key in self.seen:
                results.append(False)
            else:
                self.seen.add(key)
                results.append(True)
        return results

    def batch_mark_seen_with_ids(self, keys: list[str], representative_ids: list[str]) -> list[tuple[bool, str | None]]:
        """Mark keys as seen and return (is_new, representative_id) for each.

        For new keys, representative_id is None.
        For duplicate keys, representative_id is the ID of the first seen sample.
        """
        results = []
        for key, rep_id in zip(keys, representative_ids, strict=False):
            if key in self.seen:
                # Duplicate - return the representative ID
                rep = self.representative.get(key) if self.track_representative else None
                results.append((False, rep))
            else:
                self.seen.add(key)
                if self.track_representative:
                    self.representative[key] = rep_id
                results.append((True, None))
        return results

    def get_representative(self, key: str) -> str | None:
        """Get the representative sample ID for a given key."""
        return self.representative.get(key)

    def reset(self):
        self.seen.clear()
        self.representative.clear()


class DedupBackend:
    """Distributed deduplication backend with bucketing.

    Each bucket is maintained by a separate Ray Actor.
    Keys are routed to specific bucket actors, distributing memory across actors.

    Performance guidelines:
    - For small datasets (<1B keys): 16-64 buckets is sufficient
    - For medium datasets (1B-10B keys): 256-1000 buckets recommended
    - For large datasets (10B-100B keys): 1000-10000 buckets recommended
    - Target: Keep ~10M-100M keys per bucket for optimal performance

    Extension: For semantic deduplication, you can use cluster_id as bucket_id:
        - Cluster records first (using a refiner)
        - Pass cluster_id to get_dedup_key() or use custom bucket_id_getter
        - Each cluster bucket handles deduplication independently
    """

    def __init__(
        self,
        num_buckets: int = 2,
        name_prefix: str = "pipeline_dedup_backend",
        bucket_id_getter: Callable[[str], int] | None = None,
        track_representative: bool = False,
    ):
        """Initialize deduplication backend with bucketing.

        Args:
            num_buckets: Number of bucket actors to create (default: 16)
                        Increase for large datasets to distribute memory load.
                        See class docstring for performance guidelines.
            name_prefix: Prefix for Ray Actor names (for Ray Dashboard visibility)
            bucket_id_getter: Optional function(key: str) -> int to compute bucket_id.
                             If None, uses hash(key) % num_buckets.
            track_representative: If True, track the representative sample ID for each key.
                                 This enables returning the representative ID when duplicates are found.
        """
        self.num_buckets = num_buckets
        self.bucket_id_getter = bucket_id_getter
        self.name_prefix = name_prefix
        self.track_representative = track_representative

        # Create one actor per bucket (or reuse existing ones)
        self.bucket_actors = []
        for bucket_id in range(num_buckets):
            actor_name = f"{name_prefix}_bucket_{bucket_id}"
            try:
                actor = ray.get_actor(actor_name)
            except ValueError:
                try:
                    actor = _DedupBackendBucketActor.options(name=actor_name).remote(
                        bucket_id, track_representative=track_representative
                    )
                except ray.exceptions.ActorAlreadyExistsError:
                    actor = ray.get_actor(actor_name)
            self.bucket_actors.append(actor)

    def _get_bucket_id(self, key: str) -> int:
        """Get bucket ID for a given key."""
        if self.bucket_id_getter:
            return self.bucket_id_getter(key) % self.num_buckets
        return hash(key) % self.num_buckets

    def _get_actor(self, key: str):
        """Get the bucket actor for a given key."""
        bucket_id = self._get_bucket_id(key)
        return self.bucket_actors[bucket_id]

    def is_seen(self, key: str) -> bool:
        """Check if key has been seen."""
        actor = self._get_actor(key)
        return ray.get(actor.is_seen.remote(key))

    def mark_seen(self, key: str) -> bool:
        """Mark key as seen."""
        actor = self._get_actor(key)
        return ray.get(actor.mark_seen.remote(key))

    def batch_mark_seen(self, keys: list[str]) -> list[bool]:
        """Batch mark multiple keys as seen (grouped by bucket for efficiency)."""
        # Group keys by bucket
        bucket_keys: dict[int, list[tuple]] = {}  # bucket_id -> [(original_index, key), ...]
        for idx, key in enumerate(keys):
            bucket_id = self._get_bucket_id(key)
            if bucket_id not in bucket_keys:
                bucket_keys[bucket_id] = []
            bucket_keys[bucket_id].append((idx, key))

        # Process each bucket in parallel
        futures = {}
        for bucket_id, key_list in bucket_keys.items():
            actor = self.bucket_actors[bucket_id]
            bucket_keys_only = [k for _, k in key_list]
            futures[bucket_id] = (actor.batch_mark_seen.remote(bucket_keys_only), key_list)

        # Collect results and reconstruct original order
        results = [False] * len(keys)
        for bucket_id, (future, key_list) in futures.items():
            bucket_results = ray.get(future)
            for (orig_idx, _), result in zip(key_list, bucket_results, strict=False):
                results[orig_idx] = result

        return results

    def batch_mark_seen_with_ids(self, keys: list[str], representative_ids: list[str]) -> list[tuple[bool, str | None]]:
        """Batch mark keys as seen and return (is_new, representative_id) for each.

        For new keys, representative_id is None.
        For duplicate keys, representative_id is the ID of the first seen sample.

        Args:
            keys: List of deduplication keys
            representative_ids: List of sample identifiers (e.g., record IDs)

        Returns:
            List of (is_new, representative_id) tuples
        """
        # Group keys by bucket
        bucket_data: dict[int, list[tuple[int, str, str]]] = {}  # bucket_id -> [(orig_idx, key, rep_id), ...]
        for idx, (key, rep_id) in enumerate(zip(keys, representative_ids, strict=False)):
            bucket_id = self._get_bucket_id(key)
            if bucket_id not in bucket_data:
                bucket_data[bucket_id] = []
            bucket_data[bucket_id].append((idx, key, rep_id))

        # Process each bucket in parallel
        futures = {}
        for bucket_id, data_list in bucket_data.items():
            actor = self.bucket_actors[bucket_id]
            bucket_keys = [k for _, k, _ in data_list]
            bucket_rep_ids = [r for _, _, r in data_list]
            futures[bucket_id] = (
                actor.batch_mark_seen_with_ids.remote(bucket_keys, bucket_rep_ids),
                data_list,
            )

        # Collect results and reconstruct original order
        results: list[tuple[bool, str | None]] = [(False, None)] * len(keys)
        for bucket_id, (future, data_list) in futures.items():
            bucket_results = ray.get(future)
            for (orig_idx, _, _), result in zip(data_list, bucket_results, strict=False):
                results[orig_idx] = result

        return results

    def reset(self):
        """Reset all bucket states."""
        futures = [actor.reset.remote() for actor in self.bucket_actors]
        ray.get(futures)
