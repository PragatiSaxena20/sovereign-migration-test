import os
import time
import json
from uuid import uuid4
from typing import Any
from threading import Thread

import requests
from fastapi.requests import Request
from fastapi.responses import Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from sovereign import _request_id_ctx_var, config, get_request_id, logs, stats
from sovereign.tracing.spans import (
    TRACE_ID,
    SPAN_STACK,
    COLLECTED_SPANS,
    SERVICE_NAME as DEFAULT_SERVICE_NAME,
    add_manual_span,
)

ZIPKIN_ENDPOINT_URL = os.getenv("ZIPKIN_ENDPOINT_URL")


class RequestContextLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        response = Response("Internal server error", status_code=500)
        token = _request_id_ctx_var.set(str(uuid4()))
        try:
            response = await call_next(request)
        finally:
            req_id = get_request_id()
            response.headers["X-Request-ID"] = req_id
            logs.access_logger.queue_log_fields(REQUEST_ID=req_id)
            _request_id_ctx_var.reset(token)
        return response


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        start_time = time.time()
        response = Response("Internal server error", status_code=500)

        source_ip, source_port = "0.0.0.0", 0
        if addr := request.client:
            source_ip = addr.host
            source_port = addr.port
        if xff := request.headers.get("X-Forwarded-For"):
            source_ip = xff.split(",")[0]  # leftmost address
        logs.access_logger.queue_log_fields(
            ENVIRONMENT=config.legacy_fields.environment,
            HOST=request.headers.get("host", "-"),
            METHOD=request.method,
            PATH=request.url.path,
            QUERY=dict(request.query_params.items()),
            SOURCE_IP=source_ip,
            SOURCE_PORT=source_port,
            PID=os.getpid(),
            USER_AGENT=request.headers.get("user-agent", "-"),
            BYTES_RX=request.headers.get("content-length", "-"),
        )
        try:
            response = await call_next(request)
        finally:
            duration = time.time() - start_time
            logs.access_logger.queue_log_fields(
                BYTES_TX=response.headers.get("content-length", "-"),
                STATUS_CODE=response.status_code,
                DURATION=duration,
            )
            if "discovery" in str(request.url):
                request_info = {
                    "path": request.url.path,
                    "xds_type": response.headers.get("X-Sovereign-Requested-Type"),
                    "client_version": response.headers.get("X-Sovereign-Client-Build"),
                    "response_code": response.status_code,
                }
                tags = [
                    ":".join(map(str, [k, v]))
                    for k, v in request_info.items()
                    if v is not None
                ]
                stats.increment("discovery.rq_total", tags=tags)
                stats.timing("discovery.rq_ms", value=duration * 1000, tags=tags)
            logs.access_logger.logger.info("request")
        return response


def _send_spans_to_zipkin(spans: list[dict[str, Any]]):
    if not ZIPKIN_ENDPOINT_URL or not spans:
        return
    try:
        resp = requests.post(
            ZIPKIN_ENDPOINT_URL,
            data=json.dumps(spans),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        print(resp.text)
    except Exception:
        pass


class ZipkinMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if not ZIPKIN_ENDPOINT_URL:
            return await call_next(request)

        trace_id = uuid4().hex
        token_trace_id = TRACE_ID.set(trace_id)
        token_span_stack = SPAN_STACK.set([])

        request_span_id = uuid4().hex[:16]
        start_time = int(time.time() * 1_000_000)
        SPAN_STACK.set([request_span_id])

        response = None
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            status_code = 500
            raise
        finally:
            end_time = int(time.time() * 1_000_000)
            duration = max(1, end_time - start_time)

            request_tags = {
                "http.method": request.method,
                "http.url": str(request.url),
                "http.status_code": str(status_code),
                "http.client_ip": request.client.host if request.client else "unknown",
            }
            if status_code >= 400:
                request_tags["error"] = "true"

            add_manual_span(
                trace_id=trace_id,
                span_id=request_span_id,
                name=f"{request.method} {request.scope.get('root_path', '')}{request.scope.get('path', '')}",
                timestamp_micros=start_time,
                duration_micros=duration,
                local_endpoint_service_name=DEFAULT_SERVICE_NAME,
                parent_id=None,
                tags=request_tags,
                kind="SERVER",
            )

            spans_to_send = COLLECTED_SPANS[trace_id]
            if spans_to_send:
                # Fire and forget
                Thread(target=_send_spans_to_zipkin, args=[spans_to_send]).start()

            TRACE_ID.reset(token_trace_id)
            SPAN_STACK.reset(token_span_stack)
