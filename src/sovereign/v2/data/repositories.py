import time

from sovereign import stats
from sovereign.v2.data.data_store import ComparisonOperator, DataStoreProtocol, DataType
from sovereign.v2.types import Context, DiscoveryEntry, WorkerNode


class ContextRepository:
    def __init__(self, data_store: DataStoreProtocol):
        self.data_store: DataStoreProtocol = data_store

    @stats.timed("repository.context.get_ms")
    def get(self, name: str) -> Context | None:
        return self.data_store.get(DataType.Context, name)

    @stats.timed("v2.repository.context.get_hash_ms")
    def get_hash(self, name: str) -> int | None:
        return self.data_store.get_property(DataType.Context, name, "data_hash")

    def get_refresh_after(self, name: str) -> int | None:
        return self.data_store.get_property(DataType.Context, name, "refresh_after")

    @stats.timed("v2.repository.context.save_ms")
    def save(self, context: Context) -> bool:
        return self.data_store.set(DataType.Context, context.name, context)

    @stats.timed("v2.repository.context.update_refresh_after_ms")
    def update_refresh_after(self, name: str, refresh_after: int) -> bool:
        return self.data_store.set_property(
            DataType.Context, name, "refresh_after", refresh_after
        )


class DiscoveryEntryRepository:
    def __init__(self, data_store: DataStoreProtocol):
        self.data_store = data_store

    @stats.timed("v2.repository.discovery_entry.clear_rendering_started_at_ms")
    def clear_rendering_started_at(self, request_hash: str) -> bool:
        return self.data_store.set_property(
            DataType.DiscoveryEntry, request_hash, "rendering_started_at", None
        )

    @stats.timed("v2.repository.discovery_entry.get_ms")
    def get(self, request_hash: str) -> DiscoveryEntry | None:
        return self.data_store.get(DataType.DiscoveryEntry, request_hash)

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
        node: WorkerNode | None = self.data_store.max_by_property(
            DataType.WorkerNode, "node_id"
        )
        if node:
            return node.node_id
        return None

    @stats.timed("v2.repository.worker_node.prune_ms")
    def prune_dead_nodes(self) -> bool:
        """
        Remove any nodes that have not sent a heartbeat in the last 10 minutes.
        """
        now = int(time.time())
        return self.data_store.delete_matching(
            DataType.WorkerNode,
            "last_heartbeat",
            ComparisonOperator.LessThanOrEqualTo,
            now - 600,
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
