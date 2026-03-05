import datetime
import logging
import os
import threading
import time
from typing import Any

from croniter import croniter
from structlog.typing import FilteringBoundLogger

from sovereign.configuration import SovereignConfigv2
from sovereign.context import CronInterval, SecondsInterval, TaskInterval, stats
from sovereign.dynamic_config import Loadable
from sovereign.utils.timer import wait_until
from sovereign.v2.data.repositories import (
    ContextRepository,
    DiscoveryEntryRepository,
)
from sovereign.v2.data.worker_queue import QueueProtocol
from sovereign.v2.logging import capture_exception, get_named_logger
from sovereign.v2.types import Context, RenderDiscoveryJob


def refresh_context(
    name: str,
    node_id: str,
    config: SovereignConfigv2,
    context_repository: ContextRepository,
    discovery_job_repository: DiscoveryEntryRepository,
    queue: QueueProtocol | None = None,
) -> bool:
    with stats.timed("v2.worker.job.refresh_context_ms", tags=[f"context:{name}"]):
        logger: FilteringBoundLogger = get_named_logger(
            f"{__name__}.{refresh_context.__qualname__} ({__file__})",
            level=logging.DEBUG,
        ).bind(
            context=name,
            node_id=node_id,
            process_id=os.getpid(),
            thread_id=threading.get_ident(),
        )

        if name not in config.template_context.context:
            logger.warning("Context not found in config, skipping refresh")
            return False

        loadable = config.template_context.context[name]

        logger.info("Refreshing context")

        try:
            value: Any = loadable.load()
            new_hash = loadable.hash(value)
            old_hash = context_repository.get_hash(name)

            if old_hash != new_hash:
                stats.increment("v2.worker.context_changed", tags=[f"context:{name}"])

                logger.debug("Context changed", old_hash=old_hash, new_hash=new_hash)

                context = Context(
                    name=name,
                    data=value,
                    data_hash=new_hash,
                    last_refreshed_at=int(time.time()),
                    refresh_after=get_refresh_after(config, loadable),
                )
                context_repository.save(context)

                request_hashes: dict[str, str] = {}

                for version, version_templates in (
                    {"default": config.templates.default} | config.templates.versions
                ).items():
                    for template in version_templates:
                        if name in template.depends_on:
                            for request_hash in discovery_job_repository.find_all_request_hashes_by_template(
                                template.type
                            ):
                                request_hashes[request_hash] = template.type

                for request_hash, template in request_hashes.items():
                    logger.info(
                        "Queuing render for discovery request because context changed",
                        request_hash=request_hash,
                        template=template,
                    )
                    if queue is None:
                        logger.debug(
                            "Skipping render queueing; no queue available",
                            context=name,
                            request_hash=request_hash,
                        )
                        continue
                    queue.put(RenderDiscoveryJob(request_hash=request_hash))

            return True
        except Exception as e:
            stats.increment("v2.worker.refresh_context.error", tags=[f"context:{name}"])
            logger.exception("Failed to load context")
            capture_exception(e)
            return False


# noinspection PyUnreachableCode
def _seconds_til_next_run(task_interval: TaskInterval) -> int:
    match task_interval.value:
        case CronInterval(cron=expression):
            cron = croniter(expression)
            next_date = cron.get_next(datetime.datetime)
            return int(wait_until(next_date))
        case SecondsInterval(seconds=seconds):
            return seconds
        case _:
            return 0


def get_refresh_after(config: SovereignConfigv2, loadable: Loadable) -> int:
    interval = loadable.interval

    # get the default interval from config if not specified in loadable
    if interval is None:
        template_context_config = config.template_context
        if template_context_config.refresh_rate is not None:
            interval = str(template_context_config.refresh_rate)
        elif template_context_config.refresh_cron is not None:
            interval = template_context_config.refresh_cron
        else:
            interval = "60"

    task_interval = TaskInterval.from_str(interval)

    return int(time.time() + _seconds_til_next_run(task_interval))
