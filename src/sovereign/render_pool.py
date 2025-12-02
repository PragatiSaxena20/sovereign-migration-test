"""
Render Pool Management
----------------------

Manages the process pool for CPU-bound rendering tasks, including:
- Worker process initialization and logging
- Metrics monitoring for pool health
- Log message forwarding from worker processes
"""

import os
import threading
import traceback
from concurrent.futures.process import ProcessPoolExecutor
from functools import partial
from multiprocessing import Pipe, cpu_count

# noinspection PyProtectedMember
from multiprocessing.connection import Connection
from types import ModuleType
from typing import Any

import pydantic

from sovereign import stats, application_logger as log
from sovereign.types import DiscoveryRequest


class RenderJob(pydantic.BaseModel):
    id: str
    request: DiscoveryRequest
    context: dict[str, Any]
    modules: dict[str, str] | None = None


class RenderJobManager:
    log_rx: Connection
    log_tx: Connection

    def __init__(self):
        # Start two threads:
        # - listen for log messages from worker processes and forward to application log
        # - send metrics every 10 seconds on state of process pool
        self.log_rx, self.log_tx = Pipe(duplex=False)

        self.log_listener_thread = threading.Thread(
            target=_forward_worker_logs_to_application_logger,
            daemon=True,
            name="LogListenerThread",
        )
        self.log_listener_thread.start()

        self._metrics_stop_event = threading.Event()
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

        self.executor = ProcessPoolExecutor(max_workers=self.num_workers)

    def _send_metrics(self):
        while not self._metrics_stop_event.is_set():
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

            self._metrics_stop_event.wait(timeout=10.0)

    def submit(self, job: RenderJob, generate_func):
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
                pickleable_job_context[k] = v

        pickleable_job = RenderJob(
            id=job.id,
            request=job.request,
            context=pickleable_job_context,
            modules=modules,
        )

        future = self.executor.submit(
            generate_func, pickleable_job, partial(_worker_log, self.log_tx)
        )

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
        self._metrics_stop_event.set()
        self.metrics_thread.join(timeout=5.0)

        self.executor.shutdown(wait=True)

        self.log_tx.close()
        self.log_listener_thread.join(timeout=5.0)
        self.log_rx.close()


def _forward_worker_logs_to_application_logger(log_rx: Connection):
    while True:
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
