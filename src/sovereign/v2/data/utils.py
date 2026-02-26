from sovereign import config
from sovereign.utils.entry_point_loader import EntryPointLoader
from sovereign.v2.data.data_store import DataStoreProtocol, DataStorePurpose
from sovereign.v2.data.repositories import ContextRepository
from sovereign.v2.data.worker_queue import QueueProtocol

_data_store_web: DataStoreProtocol | None = None
_data_store_worker: DataStoreProtocol | None = None
_context_repository_web: ContextRepository | None = None
_context_repository_worker: ContextRepository | None = None
_queue: QueueProtocol | None = None


def _create_new_data_store() -> DataStoreProtocol:
    data_store = None

    entry_points = EntryPointLoader("data_stores")

    for entry_point in entry_points.groups["data_stores"]:
        if entry_point.name == config.worker_v2_data_store_provider:
            data_store = entry_point.load()()
            break

    if not data_store:
        raise ValueError(
            f"Data store '{config.worker_v2_data_store_provider}' not found in entry points"
        )

    return data_store


def get_data_store_web() -> DataStoreProtocol:
    global _data_store_web

    if not _data_store_web:
        _data_store_web = _create_new_data_store()
        _data_store_web.set_purpose(DataStorePurpose.Web)
        if not _data_store_web.migrate():
            raise RuntimeError("Data store migration failed")

    return _data_store_web


def get_data_store_worker() -> DataStoreProtocol:
    global _data_store_worker

    if not _data_store_worker:
        _data_store_worker = _create_new_data_store()
        _data_store_worker.set_purpose(DataStorePurpose.Worker)
        if not _data_store_worker.migrate():
            raise RuntimeError("Data store migration failed")

    return _data_store_worker


def get_context_repository_web() -> ContextRepository:
    global _context_repository_web

    if not _context_repository_web:
        _context_repository_web = ContextRepository(get_data_store_web())
        # _context_repository_web.load_all_into_cache()

    return _context_repository_web


def get_context_repository_worker() -> ContextRepository:
    global _context_repository_worker

    if not _context_repository_worker:
        _context_repository_worker = ContextRepository(get_data_store_worker())
        # _context_repository_worker.load_all_into_cache()

    return _context_repository_worker


def get_queue() -> QueueProtocol:
    global _queue

    if not _queue:
        entry_points = EntryPointLoader("queues")

        for entry_point in entry_points.groups["queues"]:
            if entry_point.name == config.worker_v2_queue_provider:
                _queue = entry_point.load()()
                break

        raise ValueError(
            f"Queue '{config.worker_v2_queue_provider}' not found in entry points"
        )

    return _queue
