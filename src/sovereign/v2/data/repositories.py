import copy
import pickle
import time

from sovereign import config, stats
from sovereign.v2.data.data_store import (
    ComparisonOperator,
    ContextUnpickleError,
    DataStoreProtocol,
    DataType,
)
from sovereign.v2.types import Context, DiscoveryEntry, WorkerNode


class ContextRepository:
    """Repository for Context objects with a process-level cache.

    Caches deserialized Context objects to avoid repeated pickle.loads()
    from the underlying data store. The cache is a class-level dict shared
    across all instances within the same process, validated against data_hash
    (cheap column-only query on cache hit).

    Returns deep copies to prevent callers from mutating cached objects.

    Thread safety: relies on CPython's GIL for atomic dict operations.
    The check-then-update in get() is not atomic, but the worst case is a
    redundant deserialization (two threads both miss), not data corruption.
    """

    _cache: dict[str, Context] = {}

    def __init__(self, data_store: DataStoreProtocol):
        self.data_store: DataStoreProtocol = data_store

    @stats.timed("v2.repository.context.load_all_into_cache_ms")
    def load_all_into_cache(self) -> None:
        """Load all contexts from the data store into the cache.

        Useful for warming up the cache at startup to avoid cold misses.
        """
        for name in config.template_context.context:
            self.get(name)

    @stats.timed("v2.repository.context.get_ms")
    def get(self, name: str) -> Context | None:
        cached = self._cache.get(name)
        if cached is not None:
            current_hash = self.data_store.get_property(
                DataType.Context, name, "data_hash"
            )
            if current_hash is not None and current_hash == cached.data_hash:
                stats.increment(
                    "v2.repository.context.cache.hit", tags=[f"context:{name}"]
                )
                return copy.deepcopy(cached)

        context = self.data_store.get(DataType.Context, name)
        if context is not None:
            self._cache[name] = context
        else:
            self._cache.pop(name, None)
        stats.increment("v2.repository.context.cache.miss", tags=[f"context:{name}"])
        return copy.deepcopy(context) if context is not None else None

    @stats.timed("v2.repository.context.get_hash_ms")
    def get_hash(self, name: str) -> int | None:
        return self.data_store.get_property(DataType.Context, name, "data_hash")

    @stats.timed("v2.repository.context.get_last_refreshed_at_ms")
    def get_last_refreshed_at(self, name: str) -> int | None:
        return self.data_store.get_property(DataType.Context, name, "last_refreshed_at")

    def get_refresh_after(self, name: str) -> int | None:
        return self.data_store.get_property(DataType.Context, name, "refresh_after")

    @stats.timed("v2.repository.context.save_ms")
    def save(self, context: Context) -> bool:
        # ensure that we can pickle and unpickle this context before saving it
        try:
            pickled = pickle.dumps(context.data)
            pickle.loads(pickled)
        except Exception as e:
            raise ContextUnpickleError(context.name) from e

        result = self.data_store.set(DataType.Context, context.name, context)
        if result:
            self._cache[context.name] = copy.deepcopy(context)
        else:
            self._cache.pop(context.name, None)
        return result

    @stats.timed("v2.repository.context.update_refresh_after_ms")
    def update_refresh_after(self, name: str, refresh_after: int) -> bool:
        result = self.data_store.set_property(
            DataType.Context, name, "refresh_after", refresh_after
        )
        if result:
            self._cache.pop(name, None)
        return result

    @stats.timed("v2.repository.context.delete_ms")
    def delete(self, name: str) -> bool:
        result = self.data_store.delete_matching(
            DataType.Context,
            "name",
            ComparisonOperator.EqualTo,
            name,
        )
        if result:
            self._cache.pop(name, None)
        return result

    @stats.timed("v2.repository.context.clear_value_ms")
    def clear_data(self, name: str) -> bool:
        result = self.data_store.set_property(DataType.Context, name, "data", None)
        if result:
            self._cache.pop(name, None)
        return result


class DiscoveryEntryRepository:
    def __init__(self, data_store: DataStoreProtocol):
        self.data_store = data_store

    @stats.timed("v2.repository.discovery_entry.clear_rendering_started_at_ms")
    def clear_rendering_started_at(self, request_hash: str) -> bool:
        return self.data_store.set_property_if_exists(
            DataType.DiscoveryEntry, request_hash, "rendering_started_at", None
        )

    @stats.timed("v2.repository.discovery_entry.count_entries_ms")
    def count_entries(self, max_age_seconds: int) -> int:
        """
        Count of requests in the database.
        """
        now = int(time.time())
        return self.data_store.count_matching(
            DataType.DiscoveryEntry,
            "last_requested_at",
            ComparisonOperator.GreaterThan,
            now
            - max_age_seconds,  # only count entries that have been requested in the last hour
        )

    @stats.timed("v2.repository.discovery_entry.get_ms")
    def get(self, request_hash: str) -> DiscoveryEntry | None:
        return self.data_store.get(DataType.DiscoveryEntry, request_hash)

    @stats.timed("v2.repository.discovery_entry.update_last_requested_at_ms")
    def update_last_requested_at(self, request_hash: str) -> bool:
        return self.data_store.set_property_if_exists(
            DataType.DiscoveryEntry, request_hash, "last_requested_at", int(time.time())
        )

    @stats.timed("v2.repository.discovery_entry.get_rendering_started_at_ms")
    def get_rendering_started_at(self, request_hash: str) -> int | None:
        return self.data_store.get_property(
            DataType.DiscoveryEntry, request_hash, "rendering_started_at"
        )

    @stats.timed("v2.repository.discovery_entry.get_and_check_last_rendered_at_ms")
    def get_and_check_last_rendered_at(
        self, request_hash: str, now: int, rendering_timeout_seconds: int
    ) -> tuple[bool, int | None]:
        """
        Atomically checks and claims a rendering slot for a discovery entry.

        This method implements a distributed locking mechanism for rendering. It attempts
        to set `rendering_started_at` to the current time if either:
        - No rendering has started yet (rendering_started_at is None), or
        - The previous rendering has timed out (rendering_started_at <= now - rendering_timeout_seconds)

        Returns:
            A tuple of (acquired, timestamp) where:
            - acquired: True if this caller successfully claimed the rendering slot.
            - timestamp: The rendering_started_at value (either the newly set time if acquired,
              or the existing time if another worker is already rendering).
        """
        (updated, value) = self.data_store.set_and_get_property_if_null_or_matching(
            DataType.DiscoveryEntry,
            request_hash,
            "rendering_started_at",
            now,
            ComparisonOperator.LessThanOrEqualTo,
            now - rendering_timeout_seconds,
        )
        return updated, int(value) if value else None

    @stats.timed("v2.repository.discovery_entry.find_by_template_ms")
    def find_all_request_hashes_by_template(self, template: str) -> list[str]:
        return self.data_store.find_all_matching_property(
            DataType.DiscoveryEntry,
            "template",
            ComparisonOperator.EqualTo,
            template,
            "request_hash",
        )

    @stats.timed("v2.repository.discovery_entry.save_ms")
    def save(self, entry: DiscoveryEntry) -> bool:
        stats.histogram(
            "v2.worker.repository.discovery_entry.size",
            len(entry.response.model_dump_json()) if entry.response else 0,
            tags=[f"template:{entry.template}"],
        )
        return self.data_store.set(DataType.DiscoveryEntry, entry.request_hash, entry)

    @stats.timed("v2.repository.discovery_entry.prune_stale_entries_ms")
    def prune_stale_entries(self, max_age_seconds: int) -> bool:
        """
        Delete discovery entries that have not been requested in the last max_age_seconds.
        """
        cutoff = int(time.time()) - max_age_seconds
        return self.data_store.delete_matching(
            DataType.DiscoveryEntry,
            "last_requested_at",
            ComparisonOperator.LessThanOrEqualTo,
            cutoff,
        )


class WorkerNodeRepository:
    def __init__(self, data_store: DataStoreProtocol):
        self.data_store = data_store

    @stats.timed("v2.repository.worker_node.heartbeat_ms")
    def send_heartbeat(self, node_id: str) -> bool:
        now = int(time.time())
        return self.data_store.set(
            DataType.WorkerNode,
            node_id,
            WorkerNode(node_id=node_id, last_heartbeat=now),
        )

    @stats.timed("v2.repository.worker_node.get_leader_ms")
    def get_leader_node_id(self) -> str | None:
        node: WorkerNode | None = self.data_store.min_by_property(
            DataType.WorkerNode, "node_id"
        )
        if node:
            return node.node_id
        return None

    @stats.timed("v2.repository.worker_node.prune_ms")
    def prune_dead_nodes(self) -> bool:
        """
        Remove any nodes that have not sent a heartbeat in the last 30 seconds.
        """
        now = int(time.time())
        return self.data_store.delete_matching(
            DataType.WorkerNode,
            "last_heartbeat",
            ComparisonOperator.LessThanOrEqualTo,
            now - 30,
        )

    @stats.timed("v2.repository.worker_node.count_active_ms")
    def count_active_nodes(self) -> int:
        """
        Count nodes that have sent a heartbeat in the last 10 minutes.
        """
        now = int(time.time())
        return self.data_store.count_matching(
            DataType.WorkerNode,
            "last_heartbeat",
            ComparisonOperator.GreaterThan,
            now - 600,
        )
