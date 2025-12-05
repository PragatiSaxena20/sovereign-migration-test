"""
Discovery
---------

Functions used to render and return discovery responses to Envoy proxies.

The templates are configurable. `todo See ref:Configuration#Templates`
"""

import importlib
import os
import traceback
from datetime import datetime

# noinspection PyProtectedMember
from multiprocessing.connection import Connection
from typing import Any

import yaml
from starlette.exceptions import HTTPException

# noinspection PyProtectedMember
from yaml.parser import ParserError, ScannerError  # type: ignore

from sovereign import logs, cache, stats
from sovereign.cache.types import Entry
from sovereign.configuration import config
from sovereign.render_pool import RenderJob, _restore_from_pickleable
from sovereign.types import ProcessedTemplate

writer = cache.CacheWriter()


def _debug_log(message: str) -> None:
    """Write debug message to /tmp/logs with timestamp"""
    os.makedirs("/tmp", exist_ok=True)
    timestamp = datetime.now().isoformat()
    with open("/tmp/logs", "a") as f:
        f.write(f"[{timestamp}] {message}\n")


type_urls = {
    "v2": {
        "listeners": "type.googleapis.com/envoy.api.v2.Listener",
        "clusters": "type.googleapis.com/envoy.api.v2.Cluster",
        "endpoints": "type.googleapis.com/envoy.api.v2.ClusterLoadAssignment",
        "secrets": "type.googleapis.com/envoy.api.v2.auth.Secret",
        "routes": "type.googleapis.com/envoy.api.v2.RouteConfiguration",
        "scoped-routes": "type.googleapis.com/envoy.api.v2.ScopedRouteConfiguration",
    },
    "v3": {
        "listeners": "type.googleapis.com/envoy.config.listener.v3.Listener",
        "clusters": "type.googleapis.com/envoy.config.cluster.v3.Cluster",
        "endpoints": "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment",
        "secrets": "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret",
        "routes": "type.googleapis.com/envoy.config.route.v3.RouteConfiguration",
        "scoped-routes": "type.googleapis.com/envoy.config.route.v3.ScopedRouteConfiguration",
        "runtime": "type.googleapis.com/envoy.service.runtime.v3.Runtime",
    },
}


def generate(job: RenderJob, log_tx: Connection) -> None:
    """
    Generate discovery response for a job. Runs in a worker process.
    Uses pipe to send logs to the main process.
    """

    request = job.request
    _debug_log(f"[GENERATE] Starting job {job.id} for {request}")
    log_tx.send(
        {
            "level": "INFO",
            "message": f"Starting rendering of {request} for job {job.id}",
            "pid": os.getpid(),
        }
    )

    tags = [f"type:{request.resource_type}"]
    _debug_log(f"[GENERATE] Job {job.id} - tags initialized: {tags}")

    # Modules cannot be pickled, so we need to load them ourselves
    # here
    loaded_modules = {}
    if job.modules is not None:
        _debug_log(
            f"[GENERATE] Job {job.id} - loading modules: {list(job.modules.keys())}"
        )
        for key, module_name in job.modules.items():
            loaded_modules[key] = importlib.import_module(module_name)
            _debug_log(f"[GENERATE] Job {job.id} - loaded module {key}: {module_name}")
    else:
        _debug_log(f"[GENERATE] Job {job.id} - no modules to load")

    # Restore betterproto Message objects from their serialized wrappers
    restored_context = _restore_from_pickleable(job.context)

    try:
        _debug_log(f"[GENERATE] Job {job.id} - entering timed template render block")
        with stats.timed("template.render_ms", tags=tags):
            _debug_log(f"[GENERATE] Job {job.id} - calling template.generate()")
            content = request.template.generate(
                discovery_request=request,
                host_header=request.desired_controlplane,
                resource_names=request.resources,
                **loaded_modules,
                **restored_context,
            )
            _debug_log(
                f"[GENERATE] Job {job.id} - template.generate() completed, content type: {type(content)}"
            )

            if not request.template.is_python_source:
                _debug_log(f"[GENERATE] Job {job.id} - deserializing YAML content")
                assert isinstance(content, str)
                content = deserialize_config(content)
                _debug_log(f"[GENERATE] Job {job.id} - deserialization complete")
            else:
                _debug_log(
                    f"[GENERATE] Job {job.id} - content is from Python source, skipping deserialization"
                )

            assert isinstance(content, dict)
            _debug_log(
                f"[GENERATE] Job {job.id} - filtering resources, requested: {request.resources}"
            )
            resources = filter_resources(content["resources"], request.resources)
            _debug_log(
                f"[GENERATE] Job {job.id} - filtered to {len(resources)} resources"
            )

            add_type_urls(request.api_version, request.resource_type, resources)
            _debug_log(
                f"[GENERATE] Job {job.id} - added type URLs for {request.api_version}/{request.resource_type}"
            )

            response = ProcessedTemplate(resources=resources)
            _debug_log(
                f"[GENERATE] Job {job.id} - created ProcessedTemplate with {len(response.resources)} resources"
            )
            # log_tx("DEBUG", f"Completed rendering of {request} for {job.id}")

            _debug_log(f"[GENERATE] Job {job.id} - writing to cache")
            cached, cache_result = writer.set(
                job.id,
                Entry(
                    text=response.model_dump_json(indent=None),
                    len=len(response.resources),
                    version=response.version_info,
                    node=request.node,
                ),
            )
            _debug_log(
                f"[GENERATE] Job {job.id} - cache write result: cached={cached}, result={cache_result}"
            )
            # log_tx("DEBUG", f"Cache result: {cache_result}")
            if cached:
                tags.append("result:ok")
                _debug_log(f"[GENERATE] Job {job.id} - SUCCESS: cached successfully")
            else:
                tags.append("result:cache_failed")
                _debug_log(f"[GENERATE] Job {job.id} - WARNING: cache failed")
    except Exception as e:
        _debug_log(
            f"[GENERATE] Job {job.id} - EXCEPTION: {e.__class__.__name__}: {str(e)}"
        )
        _debug_log(f"[GENERATE] Job {job.id} - traceback: {traceback.format_exc()}")
        log_tx.send(
            {
                "level": "ERROR",
                "message": f"Failed to render job for {job.id}: {traceback.format_exc()}",
                "pid": os.getpid(),
            }
        )
        tags.append("result:err")
        tags.append(f"error:{e.__class__.__name__.lower()}")
        if config.sentry_dsn.get_secret_value():
            _debug_log(f"[GENERATE] Job {job.id} - sending exception to Sentry")
            mod = importlib.import_module("sentry_sdk")
            mod.capture_exception(e)
        raise e
    finally:
        stats.increment("template.render", tags=tags)
        _debug_log(f"[GENERATE] Job {job.id} - COMPLETE: final tags={tags}")


def deserialize_config(content: str) -> dict[str, Any]:
    try:
        envoy_configuration = yaml.safe_load(content)
    except (ParserError, ScannerError) as e:
        logs.access_logger.queue_log_fields(
            error=repr(e),
            YAML_CONTEXT=e.context,
            YAML_CONTEXT_MARK=e.context_mark,
            YAML_NOTE=e.note,
            YAML_PROBLEM=e.problem,
            YAML_PROBLEM_MARK=e.problem_mark,
        )

        if config.sentry_dsn:
            mod = importlib.import_module("sentry_sdk")
            mod.capture_exception(e)

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to load configuration, there may be "
                "a syntax error in the configured templates. "
                "Please check Sentry if you have configured Sentry DSN"
            ),
        )
    if not isinstance(envoy_configuration, dict):
        raise RuntimeError(
            f"Deserialized configuration is of unexpected format: {envoy_configuration}"
        )
    return envoy_configuration


def filter_resources(
    generated: list[dict[str, Any]], requested: list[str]
) -> list[dict[str, Any]]:
    """
    If Envoy specifically requested a resource, this removes everything
    that does not match the name of the resource.
    If Envoy did not specifically request anything, every resource is retained.
    """
    if len(requested) == 0:
        return generated
    return [resource for resource in generated if resource_name(resource) in requested]


def resource_name(resource: dict[str, Any]) -> str:
    name = resource.get("name") or resource.get("cluster_name")
    if isinstance(name, str):
        return name
    raise KeyError(
        f"Failed to determine the name or cluster_name of the following resource: {resource}"
    )


def add_type_urls(api_version, resource_type, resources):
    type_url = type_urls.get(api_version, {}).get(resource_type)
    if type_url is not None:
        for resource in resources:
            if not resource.get("@type"):
                resource["@type"] = type_url
