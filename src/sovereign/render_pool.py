"""
Render Pool Management
----------------------

Manages the process pool for CPU-bound rendering tasks, including:
- Worker process initialization and logging
- Metrics monitoring for pool health
- Log message forwarding from worker processes
"""

import faulthandler
import os
import sys
import threading
import traceback
from concurrent.futures.process import ProcessPoolExecutor
from multiprocessing import Pipe, cpu_count

# noinspection PyProtectedMember
from multiprocessing.connection import Connection
from types import ModuleType
from typing import Any, Callable

import betterproto
import pydantic
from betterproto import Casing

from sovereign import stats, application_logger as log
from sovereign.types import DiscoveryRequest


class _SerialisedMessage:
    # noinspection PyTypeChecker
    def __init__(self, message: betterproto.Message):
        # Don't include default values or our deserialisation is likely to select the wrong field to populate in case
        # of 'oneof' fields
        self.data = message.to_dict(casing=Casing.SNAKE, include_default_values=False)
        self.module_name = type(message).__module__
        self.class_name = type(message).__qualname__

    def deserialise(self) -> betterproto.Message:
        import importlib
        module = importlib.import_module(self.module_name)
        cls = module
        for part in self.class_name.split('.'):
            cls = getattr(cls, part)
        # noinspection PyCallingNonCallable
        return cls().from_dict(self.data)


def _make_pickleable(obj: Any) -> Any:
    if isinstance(obj, betterproto.Message):
        return _SerialisedMessage(obj)
    elif isinstance(obj, dict):
        return {k: _make_pickleable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_make_pickleable(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(_make_pickleable(item) for item in obj)
    elif isinstance(obj, set):
        return {_make_pickleable(item) for item in obj}
    else:
        return obj


def _restore_from_pickleable(obj: Any) -> Any:
    if isinstance(obj, _SerialisedMessage):
        return obj.deserialise()
    elif isinstance(obj, dict):
        return {k: _restore_from_pickleable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_restore_from_pickleable(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(_restore_from_pickleable(item) for item in obj)
    elif isinstance(obj, set):
        return {_restore_from_pickleable(item) for item in obj}
    else:
        return obj


def _worker_initialiser(log_tx: Connection):
    faulthandler.enable()
    original_excepthook = sys.excepthook

    def _excepthook(exc_type, exc_value, exc_tb):
        try:
            tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            log_tx.send(
                {
                    "level": "ERROR",
                    "message": f"Worker process terminating with uncaught exception: {tb_str}",
                    "pid": os.getpid(),
                }
            )
        except Exception:
            pass

        original_excepthook(exc_type, exc_value, exc_tb)

    sys.excepthook = _excepthook


class RenderJob(pydantic.BaseModel):
    id: str
    request: DiscoveryRequest
    context: dict[str, Any]
    modules: dict[str, str] | None = None


class RenderJobManager:
    log_rx: Connection
    log_tx: Connection
    _stop_event: threading.Event

    def __init__(self):
        # Start two threads:
        # - listen for log messages from worker processes and forward to application log
        # - send metrics every 10 seconds on state of process pool
        self.log_rx, self.log_tx = Pipe(duplex=False)

        # Used to signal threads to stop
        self._stop_event = threading.Event()

        self.log_listener_thread = threading.Thread(
            target=_forward_worker_logs_to_application_logger,
            args=[self.log_rx, self._stop_event],
            daemon=True,
            name="LogListenerThread",
        )
        self.log_listener_thread.start()

        self.metrics_thread = threading.Thread(
            target=self._send_metrics,
            daemon=True,
            name="RenderPoolMetricsThread",
        )
        self.metrics_thread.start()

        # Limit render jobs to number of cores - 2 (minimum of 1) to ensure
        # that we leave some cores available for the web servers (one for
        # the main HTTP server, and one for the worker's HTTP server).
        self.num_workers = max(1, cpu_count() - 2)
        log.info(
            f"[RenderJobManager] {self.num_workers} workers (total cores: {cpu_count()})"
        )

        self.executor = ProcessPoolExecutor(
            max_workers=self.num_workers,
            initializer=_worker_initialiser,
            initargs=(self.log_tx,),
        )

    def _send_metrics(self):
        while not self._stop_event.is_set():
            try:
                if self.executor is not None:
                    stats.gauge(
                        "render_pool.queue_size", len(self.executor._pending_work_items)
                    )
                    stats.gauge(
                        "render_pool.num_processes", len(self.executor._processes)
                    )
                    stats.gauge("render_pool.max_processes", self.num_workers)
            except Exception:
                pass

            self._stop_event.wait(timeout=10.0)

    def submit(
        self, job: RenderJob, generate_func: Callable[[RenderJob, Connection], None]
    ):
        log.debug(
            f"[RenderJobManager] Submitting render job {job.id} for resource type: {job.request.resource_type}"
        )

        # Modules cannot be pickled, so we need to pull them out of the
        # context and then add them to the job to be loaded in the
        # worker sub-process.
        modules = {}
        pickleable_job_context = {}

        for k, v in job.context.items():
            if isinstance(v, ModuleType):  # check if it's a module
                modules[k] = v.__name__
            else:
                # Convert betterproto Message objects to dicts to survive pickling
                pickleable_job_context[k] = _make_pickleable(v)

        pickleable_job = RenderJob(
            id=job.id,
            request=job.request,
            context=pickleable_job_context,
            modules=modules,
        )

        future = self.executor.submit(generate_func, pickleable_job, self.log_tx)

        def log_result(fut):
            try:
                fut.result()  # This will raise if the job failed
            except Exception as e:
                log.error(
                    f"[RenderJobManager] Render job {job.id} failed with exception: {e}"
                )
                log.error(f"[RenderJobManager] Traceback: {traceback.format_exc()}")

        future.add_done_callback(log_result)
        log.debug(f"[RenderJobManager] Render job {job.id} submitted to executor")

    def shutdown(self):
        self._stop_event.set()
        self.metrics_thread.join(timeout=5.0)

        self.executor.shutdown(wait=True)

        self.log_tx.close()
        self.log_listener_thread.join(timeout=5.0)
        self.log_rx.close()


def _forward_worker_logs_to_application_logger(
    log_rx: Connection, stop_event: threading.Event
):
    while not stop_event.is_set():
        try:
            if log_rx.poll(timeout=1.0):
                msg = log_rx.recv()
                level = msg.get("level", "INFO")
                message = msg.get("message", "")
                pid = msg.get("pid", "unknown")

                # Forward to application logger
                log_msg = f"[Worker {pid}] {message}"
                if level == "DEBUG":
                    log.debug(log_msg)
                elif level == "INFO":
                    log.info(log_msg)
                elif level == "WARNING":
                    log.warning(log_msg)
                elif level == "ERROR":
                    log.error(log_msg)
                else:
                    log.info(log_msg)
        except EOFError:
            break
        except Exception:
            pass


def _worker_log(log_tx, level: str, message: str):
    try:
        log_tx.send({"level": level, "message": message, "pid": os.getpid()})
    except Exception:
        pass
