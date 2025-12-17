import asyncio
from typing import final
from contextlib import asynccontextmanager

from fastapi import FastAPI, Body

from sovereign import (
    cache,
    rendering,
    server_cipher_container,
    disabled_ciphersuite,
    application_logger as log,
    stats,
)
from sovereign.context import TemplateContext
from sovereign.render_pool import RenderJobManager, RenderJob
from sovereign.sources import SourcePoller
from sovereign.configuration import config
from sovereign.types import RegisterClientRequest, DiscoveryRequest
from sovereign.events import bus, Topic


# noinspection PyUnusedLocal
def hidden_field(*args, **kwargs):
    return "(value hidden)"


def shown_field(value):
    return value


def inject_builtin_items(request, output):
    output["__hide_from_ui"] = shown_field
    output["crypto"] = server_cipher_container
    if request.is_internal_request:
        output["__hide_from_ui"] = hidden_field
        output["crypto"] = disabled_ciphersuite


template_context = TemplateContext.from_config()
context_middleware = [inject_builtin_items]
template_context.middleware = context_middleware
writer = cache.CacheWriter()

ClientId = str
OnDemandJob = tuple[ClientId, DiscoveryRequest]


@final
class RenderQueue:
    def __init__(self, maxsize: int = 0):
        self._queue: asyncio.Queue[OnDemandJob] = asyncio.Queue(maxsize)
        self._set: set[ClientId] = set()
        self._lock = asyncio.Lock()

    async def put(self, item: OnDemandJob):
        cid = item[0]
        async with self._lock:
            if cid not in self._set:
                await self._queue.put(item)
                self._set.add(cid)

    def put_nowait(self, item: OnDemandJob):
        cid = item[0]
        if cid in self._set:
            return
        if self._queue.full():
            raise asyncio.QueueFull
        self._queue.put_nowait(item)
        self._set.add(cid)

    async def get(self):
        return await self._queue.get()

    def full(self):
        return self._queue.full()

    async def task_done(self, cid):
        async with self._lock:
            self._set.remove(cid)
        self._queue.task_done()


RENDER_ONDEMAND_QUEUE = RenderQueue()
RENDER_BACKGROUND_JOB_MANAGER = RenderJobManager()

poller = None
if config.sources is not None:
    if config.matching is not None:
        matching_enabled = config.matching.enabled
        node_key: str | None = config.matching.node_key
        source_key: str | None = config.matching.source_key
    else:
        matching_enabled = False
        node_key = None
        source_key = None
    poller = SourcePoller(
        sources=config.sources,
        matching_enabled=matching_enabled,
        node_match_key=node_key,
        source_match_key=source_key,
        source_refresh_rate=config.source_config.refresh_rate,
        logger=log,
        stats=stats,
    )
    context_middleware.append(poller.add_to_context)


async def render_on_event(ctx):
    subscription = bus.subscribe(Topic.CONTEXT)
    log.info("[render_on_event] Started, waiting for context events")
    while True:
        # block forever until new context arrives
        log.debug("[render_on_event] Waiting for next context event")
        event = await subscription.get()
        context_name = event.metadata.get("name")

        log.info(f"[render_on_event] Received context event: {context_name}")
        log.debug(event.message)
        try:
            if registered := writer.get_registered_clients():
                size = len(registered)
                log.info(f"[render_on_event] Found {size} registered clients to potentially render")
                stats.increment("template.render_on_event", tags=[f"batch_size:{size}"])

                rendered_count = 0
                skipped_count = 0
                for client, request in registered:
                    if context_name in request.template.depends_on:
                        log.debug(f"[render_on_event] Client {client} depends on {context_name}, rendering template for {request}")
                        job = RenderJob(
                            id=client,
                            request=request,
                            context=ctx.get_context(request),
                        )
                        RENDER_BACKGROUND_JOB_MANAGER.submit(job, rendering.generate)
                        rendered_count += 1
                    else:
                        log.debug(f"[render_on_event] Client {client} does not depend on {context_name}, skipping")
                        skipped_count += 1
                log.info(f"[render_on_event] Submitted {rendered_count} render jobs, skipped {skipped_count} clients")
            else:
                log.debug("[render_on_event] No registered clients found")
        except Exception as e:
            log.error(f"[render_on_event] Error processing context event: {e}", exc_info=True)
        finally:
            log.debug(f"[render_on_event] Entering cooldown for {config.template_context.cooldown} seconds")
            await asyncio.sleep(config.template_context.cooldown)


async def render_on_demand(ctx):
    log.info("[render_on_demand] Started, waiting for on-demand render requests")
    while True:
        # noinspection PyShadowingBuiltins
        log.debug("[render_on_demand] Waiting for next request from queue")
        id, request = await RENDER_ONDEMAND_QUEUE.get()
        stats.increment("template.render_on_demand")
        log.info(
            f"[render_on_demand] Received on-demand request to render templates for {id} ({request})"
        )
        try:
            log.debug(f"[render_on_demand] Getting context for request {id}")
            context = ctx.get_context(request)
            log.debug(f"[render_on_demand] Context retrieved for {id}, context keys: {list(context.keys())}")
            job = RenderJob(id=id, request=request, context=context)
            log.debug(f"[render_on_demand] Submitting job {id} to background job manager")
            RENDER_BACKGROUND_JOB_MANAGER.submit(job, rendering.generate)
            log.info(f"[render_on_demand] Job {id} submitted successfully")
        except Exception as e:
            log.error(f"[render_on_demand] Error processing on-demand request {id}: {e}", exc_info=True)
        finally:
            await RENDER_ONDEMAND_QUEUE.task_done(id)
            log.debug(f"[render_on_demand] Task done for {id}")


async def monitor_render_queue():
    """Periodically report render queue size metrics"""
    while True:
        await asyncio.sleep(10)
        # noinspection PyProtectedMember
        stats.gauge(
            "template.on_demand_queue_size", RENDER_ONDEMAND_QUEUE._queue.qsize()
        )


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Template Rendering
    log.debug("Starting rendering loops")
    asyncio.create_task(render_on_event(template_context))
    asyncio.create_task(render_on_demand(template_context))
    asyncio.create_task(monitor_render_queue())

    # Template context
    subscription = bus.subscribe(Topic.CONTEXT)
    log.debug("Starting context loop")
    asyncio.create_task(template_context.start())
    event = await subscription.get()
    log.debug(event.message)

    # Source polling
    if poller is not None:
        log.debug("Starting source poller")
        poller.lazy_load_modifiers(config.modifiers)
        poller.lazy_load_global_modifiers(config.global_modifiers)
        asyncio.create_task(poller.poll_forever())

    log.debug("Worker lifespan initialized")
    yield

    # Cleanup
    log.debug("Shutting down render job manager")
    RENDER_BACKGROUND_JOB_MANAGER.shutdown()


worker = FastAPI(lifespan=lifespan)
if dsn := config.sentry_dsn.get_secret_value():
    try:
        # noinspection PyUnusedImports
        import sentry_sdk

        # noinspection PyUnusedImports
        from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

        sentry_sdk.init(dsn)
        worker.add_middleware(SentryAsgiMiddleware)  # type: ignore
    except ImportError:  # pragma: no cover
        log.error("Sentry DSN configured but failed to attach to worker")


@worker.get("/health")
def health():
    return "OK"


@worker.put("/client")
async def client_add(
    registration: RegisterClientRequest = Body(...),
):
    log.info(f"Received registration: {registration.request}")
    xds = registration.request
    id, req = writer.register(xds)
    ONDEMAND.put_nowait((id, req))
    return "Registered", 200
