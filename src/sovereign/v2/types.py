import time
from typing import Any

import pydantic
from pydantic import TypeAdapter

from sovereign.types import DiscoveryRequest, DiscoveryResponse


class Context(pydantic.BaseModel):
    name: str
    data: Any
    data_hash: int
    last_refreshed_at: int | None = None
    refresh_after: int


class DiscoveryEntry(pydantic.BaseModel):
    request_hash: str
    template: str
    request: DiscoveryRequest
    response: DiscoveryResponse | None
    last_rendered_at: int | None = None
    rendering_started_at: int | None = None


class RefreshContextJob(pydantic.BaseModel):
    context_name: str
    created_at: float = pydantic.Field(default_factory=time.time)


class RenderDiscoveryJob(pydantic.BaseModel):
    request_hash: str
    created_at: float = pydantic.Field(default_factory=time.time)


QueueJob = RefreshContextJob | RenderDiscoveryJob


queue_job_type_adapter = TypeAdapter(QueueJob)


class WorkerNode(pydantic.BaseModel):
    node_id: str
    last_heartbeat: int
