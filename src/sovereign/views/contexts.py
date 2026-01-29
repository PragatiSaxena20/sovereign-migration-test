from datetime import datetime
from typing import Annotated, Any

import pydantic
from fastapi import APIRouter, Path
from fastapi.responses import JSONResponse

from sovereign.configuration import config
from sovereign.v2.data.repositories import ContextHistoryRepository
from sovereign.v2.data.utils import get_data_store

router = APIRouter()


class ContextHistoryEntryResponse(pydantic.BaseModel):
    created_at: int
    created_at_formatted: str
    data_hash: int
    data: Any


class ContextHistoryResponse(pydantic.BaseModel):
    entries: list[ContextHistoryEntryResponse]
    total_entries: int


_data_store = get_data_store()
_context_history_repository = ContextHistoryRepository(_data_store)


@router.get(
    "/{context_name}/history",
    summary="Get context history",
    response_model=ContextHistoryResponse,
)
async def get_context_history(
    context_name: Annotated[str, Path(title="Name of the context to query")],
) -> JSONResponse:
    if not config.worker_v2_enabled:
        return JSONResponse(
            status_code=400,
            content={
                "title": "Feature unavailable",
                "detail": "Context history is only available when worker_v2 is enabled",
            },
        )

    history_entries = _context_history_repository.get_history(context_name)

    entries = [
        ContextHistoryEntryResponse(
            created_at=entry.created_at,
            created_at_formatted=datetime.fromtimestamp(entry.created_at).isoformat(),
            data_hash=entry.data_hash,
            data=entry.data,
        )
        for entry in history_entries
    ]

    response = ContextHistoryResponse(
        entries=entries,
        total_entries=len(entries),
    )

    return JSONResponse(content=response.model_dump())
