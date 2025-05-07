import os
import time
import uuid
import functools
import traceback
import asyncio
import socket
from contextvars import ContextVar
from typing import List, Dict, Any, Optional, Callable
from collections import defaultdict

TRACE_ID: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)
COLLECTED_SPANS: dict[str, List[Dict[str, Any]]] = defaultdict(list)
# Mainly a way to access parent span ids
SPAN_STACK: ContextVar[List[str]] = ContextVar("span_stack", default=[])

SERVICE_NAME = os.getenv("SERVICE_NAME", "unknown-service")


try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0.1)
    s.connect(("1.1.1.1", 53))
    LOCAL_IP = s.getsockname()[0]
    s.close()
except Exception:
    LOCAL_IP = "127.0.0.1"


def _get_current_parent_id() -> Optional[str]:
    stack = SPAN_STACK.get()
    return stack[-1] if stack else None


def zipkin_span(
    name: Optional[str] = None, service_name_override: Optional[str] = None
):
    def decorator(func: Callable):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            trace_id = TRACE_ID.get()
            if not trace_id:
                return await func(*args, **kwargs)

            span_name = name or func.__name__
            service_name = service_name_override or SERVICE_NAME

            parent_id = _get_current_parent_id()
            span_id = uuid.uuid4().hex[:16]

            current_stack = SPAN_STACK.get()[:]  # Make a copy to modify
            current_stack.append(span_id)
            token_stack = SPAN_STACK.set(current_stack)

            error_info = None
            start_time = int(time.time() * 1_000_000)
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                error_info = {
                    "error": str(e),
                    "error.kind": e.__class__.__name__,
                    "error.stack": traceback.format_exc(),
                }
                raise
            finally:
                end_time = int(time.time() * 1_000_000)
                duration = max(1, end_time - start_time)
                span_data = {
                    "traceId": trace_id,
                    "id": span_id,
                    "name": span_name,
                    "timestamp": start_time,
                    "duration": duration,
                    "localEndpoint": {"serviceName": service_name, "ipv4": LOCAL_IP},
                    "tags": {},
                }
                if parent_id:
                    span_data["parentId"] = parent_id

                if error_info:
                    span_data["tags"]["error"] = "true"
                    span_data["tags"]["python.exception.type"] = error_info[
                        "error.kind"
                    ]
                    span_data["tags"]["python.exception.message"] = error_info["error"]

                collected_spans_list = COLLECTED_SPANS[trace_id]
                collected_spans_list.append(span_data)
                SPAN_STACK.reset(token_stack)

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            trace_id = TRACE_ID.get()
            if not trace_id:
                return func(*args, **kwargs)

            span_name = name or func.__name__
            current_service_name = service_name_override or SERVICE_NAME
            parent_id = _get_current_parent_id()
            span_id = uuid.uuid4().hex[:16]

            current_stack = SPAN_STACK.get()[:]
            current_stack.append(span_id)
            token_stack = SPAN_STACK.set(current_stack)

            start_time = int(time.time() * 1_000_000)
            error_info = None
            result = None

            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                error_info = {
                    "error": str(e),
                    "error.kind": e.__class__.__name__,
                    "error.stack": traceback.format_exc(),
                }
                raise
            finally:
                end_time = int(time.time() * 1_000_000)
                duration = max(1, end_time - start_time)
                span_data = {
                    "traceId": trace_id,
                    "id": span_id,
                    "name": span_name,
                    "timestamp": start_time,
                    "duration": duration,
                    "localEndpoint": {
                        "serviceName": current_service_name,
                        "ipv4": LOCAL_IP,
                    },
                    "tags": {},
                }
                if parent_id:
                    span_data["parentId"] = parent_id
                if error_info:
                    span_data["tags"]["error"] = "true"
                    span_data["tags"]["python.exception.type"] = error_info[
                        "error.kind"
                    ]
                    span_data["tags"]["python.exception.message"] = error_info["error"]

                collected_spans_list = COLLECTED_SPANS[trace_id]
                collected_spans_list.append(span_data)
                SPAN_STACK.reset(token_stack)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def add_manual_span(
    trace_id: str,
    span_id: str,
    name: str,
    timestamp_micros: int,
    duration_micros: int,
    local_endpoint_service_name: str,
    parent_id: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    annotations: Optional[List[Dict[str, Any]]] = None,
    kind: Optional[str] = None,
):
    if not TRACE_ID.get():
        return

    span_data = {
        "traceId": trace_id,
        "id": span_id,
        "name": name,
        "timestamp": timestamp_micros,
        "duration": duration_micros,
        "localEndpoint": {"serviceName": local_endpoint_service_name, "ipv4": LOCAL_IP},
        "tags": tags or {},
    }
    if parent_id:
        span_data["parentId"] = parent_id
    if kind:
        span_data["kind"] = kind.upper()
    if annotations:
        span_data["annotations"] = annotations

    collected_spans_list = COLLECTED_SPANS[trace_id]
    collected_spans_list.append(span_data)
