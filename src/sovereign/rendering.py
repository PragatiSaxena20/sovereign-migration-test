"""
Discovery
---------

Functions used to render and return discovery responses to Envoy proxies.

The templates are configurable. `todo See ref:Configuration#Templates`
"""

import importlib
import os
import traceback

# noinspection PyProtectedMember
from multiprocessing.connection import Connection
from typing import Any

import yaml
from starlette.exceptions import HTTPException

# noinspection PyProtectedMember
from yaml.parser import ParserError, ScannerError  # type: ignore

from sovereign import cache, logs, stats
from sovereign.cache.types import Entry
from sovereign.configuration import config
from sovereign.render_pool import RenderJob
from sovereign.types import ProcessedTemplate, TemplateMissingArgsError

writer = cache.CacheWriter()


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
    log_tx.send(
        {
            "level": "INFO",
            "message": f"Starting rendering of {request} for job {job.id}",
            "pid": os.getpid(),
            "job_id": job.id,
            "resource_type": request.resource_type,
            "phase": "start",
        }
    )

    tags = [f"type:{request.resource_type}"]

    # Modules cannot be pickled, so we need to load them ourselves
    # here
    loaded_modules = {}
    if job.modules is not None:
        log_tx.send(
            {
                "level": "DEBUG",
                "message": f"Loading {len(job.modules)} modules for job {job.id}",
                "pid": os.getpid(),
                "job_id": job.id,
                "modules": list(job.modules.keys()),
                "phase": "loading_modules",
            }
        )
        for key, module_name in job.modules.items():
            loaded_modules[key] = importlib.import_module(module_name)
        log_tx.send(
            {
                "level": "DEBUG",
                "message": f"Finished loading modules for job {job.id}",
                "pid": os.getpid(),
                "job_id": job.id,
                "phase": "modules_loaded",
            }
        )

    # Restore betterproto Message objects from their serialized wrappers
    log_tx.send(
        {
            "level": "DEBUG",
            "message": f"Restoring context from pickleable for job {job.id}",
            "pid": os.getpid(),
            "job_id": job.id,
            "phase": "restoring_context",
        }
    )
    log_tx.send(
        {
            "level": "DEBUG",
            "message": f"Context restored for job {job.id}, context keys: {list(job.context.keys())}",
            "pid": os.getpid(),
            "job_id": job.id,
            "context_keys": list(job.context.keys()),
            "phase": "context_restored",
        }
    )

    try:
        with stats.timed("template.render_ms", tags=tags):
            log_tx.send(
                {
                    "level": "DEBUG",
                    "message": f"Starting template generation for job {job.id}",
                    "pid": os.getpid(),
                    "job_id": job.id,
                    "template_type": request.template.resource_type
                    if hasattr(request.template, "resource_type")
                    else "unknown",
                    "is_python_source": request.template.is_python_source
                    if hasattr(request.template, "is_python_source")
                    else "unknown",
                    "phase": "template_generate_start",
                }
            )
            try:
                content = request.template.generate(
                    discovery_request=request,
                    host_header=request.desired_controlplane,
                    resource_names=request.resources,
                    **loaded_modules,
                    **job.context,
                )
            except TemplateMissingArgsError as e:
                log_tx.send(
                    {
                        "level": "WARNING",
                        "message": (
                            f"Template for resource type {request.template.resource_type} for job "
                            f"{job.id} missing args {e.missing_args}, cannot render"
                        ),
                        "pid": os.getpid(),
                        "job_id": job.id,
                        "phase": "template_missing_args",
                    }
                )
                tags.append("result:missing_args")
                raise e

            log_tx.send(
                {
                    "level": "DEBUG",
                    "message": f"Template generated for job {job.id}, content type: {type(content).__name__}",
                    "pid": os.getpid(),
                    "job_id": job.id,
                    "content_type": type(content).__name__,
                    "phase": "template_generated",
                }
            )

            if not request.template.is_python_source:
                log_tx.send(
                    {
                        "level": "DEBUG",
                        "message": f"Deserializing YAML config for job {job.id}",
                        "pid": os.getpid(),
                        "job_id": job.id,
                        "content_length": len(content)
                        if isinstance(content, str)
                        else 0,
                        "phase": "deserialize_start",
                    }
                )
                assert isinstance(content, str)
                content = deserialize_config(content)
                log_tx.send(
                    {
                        "level": "DEBUG",
                        "message": f"YAML config deserialized for job {job.id}",
                        "pid": os.getpid(),
                        "job_id": job.id,
                        "phase": "deserialize_complete",
                    }
                )

            assert isinstance(content, dict)
            log_tx.send(
                {
                    "level": "DEBUG",
                    "message": f"Filtering resources for job {job.id}",
                    "pid": os.getpid(),
                    "job_id": job.id,
                    "total_resources": len(content.get("resources", [])),
                    "requested_resources": request.resources,
                    "phase": "filter_resources_start",
                }
            )
            resources = filter_resources(content["resources"], request.resources)
            log_tx.send(
                {
                    "level": "DEBUG",
                    "message": f"Resources filtered for job {job.id}",
                    "pid": os.getpid(),
                    "job_id": job.id,
                    "filtered_count": len(resources),
                    "phase": "filter_resources_complete",
                }
            )

            log_tx.send(
                {
                    "level": "DEBUG",
                    "message": f"Adding type URLs for job {job.id}",
                    "pid": os.getpid(),
                    "job_id": job.id,
                    "api_version": request.api_version,
                    "resource_type": request.resource_type,
                    "phase": "add_type_urls",
                }
            )
            add_type_urls(request.api_version, request.resource_type, resources)

            response = ProcessedTemplate(resources=resources)

            log_tx.send(
                {
                    "level": "DEBUG",
                    "message": f"Writing to cache for job {job.id}",
                    "pid": os.getpid(),
                    "job_id": job.id,
                    "resource_count": len(response.resources),
                    "version_info": response.version_info,
                    "phase": "cache_write_start",
                }
            )
            cached, cache_result = writer.set(
                job.id,
                Entry(
                    text=response.model_dump_json(indent=None),
                    len=len(response.resources),
                    version=response.version_info,
                    node=request.node,
                ),
            )
            if cached:
                log_tx.send(
                    {
                        "level": "INFO",
                        "message": f"Successfully cached response for job {job.id}",
                        "pid": os.getpid(),
                        "job_id": job.id,
                        "resource_count": len(response.resources),
                        "cache_result": str(cache_result),
                        "phase": "cache_write_success",
                    }
                )
                tags.append("result:ok")
            else:
                log_tx.send(
                    {
                        "level": "WARNING",
                        "message": f"Failed to cache response for job {job.id}",
                        "pid": os.getpid(),
                        "job_id": job.id,
                        "cache_result": str(cache_result),
                        "phase": "cache_write_failed",
                    }
                )
                tags.append("result:cache_failed")
    except Exception as e:
        log_tx.send(
            {
                "level": "ERROR",
                "message": f"Failed to render job for {job.id}: {traceback.format_exc()}",
                "pid": os.getpid(),
                "job_id": job.id,
                "error_type": e.__class__.__name__,
                "error_message": str(e),
                "phase": "render_error",
            }
        )
        tags.append("result:err")
        tags.append(f"error:{e.__class__.__name__.lower()}")
        if config.sentry_dsn.get_secret_value():
            mod = importlib.import_module("sentry_sdk")
            mod.capture_exception(e)
        raise e
    finally:
        log_tx.send(
            {
                "level": "DEBUG",
                "message": f"Finished processing job {job.id}",
                "pid": os.getpid(),
                "job_id": job.id,
                "tags": tags,
                "phase": "complete",
            }
        )
        stats.increment("template.render", tags=tags)


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
