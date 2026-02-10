from unittest.mock import patch

import pytest
from starlette_context import context

from sovereign.types import DiscoveryResponse
from sovereign.utils.mock import mock_discovery_request
from sovereign.v2.data.data_store import InMemoryDataStore
from sovereign.v2.data.repositories import DiscoveryEntryRepository
from sovereign.v2.data.worker_queue import InMemoryQueue
from sovereign.v2.types import DiscoveryEntry
from sovereign.v2.web import wait_for_discovery_response


@pytest.fixture(scope="function")
def data_store() -> InMemoryDataStore:
    return InMemoryDataStore()


@pytest.fixture(scope="function")
def queue() -> InMemoryQueue:
    return InMemoryQueue()


@pytest.fixture
def mock_response() -> DiscoveryResponse:
    return DiscoveryResponse(
        version_info="test_version_123",
        resources=[{"@type": "test", "name": "test_resource"}],
    )


@pytest.mark.asyncio
async def test_render_inline_renders_inline(
    data_store: InMemoryDataStore,
    queue: InMemoryQueue,
    mock_response: DiscoveryResponse,
):
    """When sovereign.render_inline is set in metadata, render inline without persisting."""
    request = mock_discovery_request(
        resource_type="clusters",
        metadata={"sovereign": {"render_inline": True}},
    )

    with (
        patch("sovereign.v2.web.get_data_store_web", return_value=data_store),
        patch("sovereign.v2.web.get_queue", return_value=queue),
        patch(
            "sovereign.v2.web.render_template_to_response",
            return_value=mock_response,
        ) as mock_render,
    ):
        result = await wait_for_discovery_response(request)

    # render_template_to_response should have been called
    mock_render.assert_called_once()

    # should return the rendered response
    assert result is not None
    assert result.version_info == "test_version_123"

    # no DiscoveryEntry should have been created
    assert queue.is_empty(), "No render job should have been queued"

    # XDS_RESPONSE_SOURCE should be set to inline
    assert context.data.get("XDS_RESPONSE_SOURCE") == "inline"


# noinspection DuplicatedCode
@pytest.mark.asyncio
async def test_no_render_inline_uses_normal_flow(
    data_store: InMemoryDataStore,
    queue: InMemoryQueue,
    mock_response: DiscoveryResponse,
):
    """Without sovereign.render_inline, the normal DB lookup + queue flow is used."""
    request = mock_discovery_request(
        resource_type="clusters",
        metadata={"auth": "test_auth"},
    )

    with (
        patch("sovereign.v2.web.get_data_store_web", return_value=data_store),
        patch("sovereign.v2.web.get_queue", return_value=queue),
        patch(
            "sovereign.v2.web.render_template_to_response",
        ) as mock_render,
        patch("sovereign.v2.web.config") as mock_config,
    ):
        mock_config.cache.hash_rules = []
        mock_config.cache.read_timeout = 0.1
        mock_config.cache.poll_interval_secs = 0.05
        await wait_for_discovery_response(request)

    # render_template_to_response should NOT have been called directly
    mock_render.assert_not_called()

    # A render job should have been queued (normal flow)
    assert not queue.is_empty(), "A render job should have been queued"


# noinspection DuplicatedCode
@pytest.mark.asyncio
async def test_empty_render_inline_uses_normal_flow(
    data_store: InMemoryDataStore,
    queue: InMemoryQueue,
):
    """Empty/falsy render_inline means normal flow is used."""
    request = mock_discovery_request(
        resource_type="clusters",
        metadata={"sovereign": {"render_inline": ""}, "auth": "test_auth"},
    )

    with (
        patch("sovereign.v2.web.get_data_store_web", return_value=data_store),
        patch("sovereign.v2.web.get_queue", return_value=queue),
        patch(
            "sovereign.v2.web.render_template_to_response",
        ) as mock_render,
        patch("sovereign.v2.web.config") as mock_config,
    ):
        mock_config.cache.hash_rules = []
        mock_config.cache.read_timeout = 0.1
        mock_config.cache.poll_interval_secs = 0.05
        await wait_for_discovery_response(request)

    # render_template_to_response should NOT have been called (empty string is falsy)
    mock_render.assert_not_called()

    # Normal flow should have been used
    assert not queue.is_empty(), "A render job should have been queued"


@pytest.mark.asyncio
async def test_from_db_sets_immediately_in_context(
    data_store: InMemoryDataStore,
    queue: InMemoryQueue,
    mock_response: DiscoveryResponse,
):
    """When a cached DiscoveryEntry exists with a response, XDS_RESPONSE_SOURCE is 'immediately'."""
    request = mock_discovery_request(
        resource_type="clusters",
        metadata={"auth": "test_auth"},
    )

    # Pre-populate a cached entry in the data store
    discovery_entry_repo = DiscoveryEntryRepository(data_store)

    with (
        patch("sovereign.v2.web.config") as mock_config,
    ):
        mock_config.cache.hash_rules = []
        request_hash = request.cache_key([])

    cached_entry = DiscoveryEntry(
        request_hash=request_hash,
        template="clusters",
        request=request,
        response=mock_response,
    )
    discovery_entry_repo.save(cached_entry)

    with (
        patch("sovereign.v2.web.get_data_store_web", return_value=data_store),
        patch("sovereign.v2.web.get_queue", return_value=queue),
        patch("sovereign.v2.web.config") as mock_config,
    ):
        mock_config.cache.hash_rules = []
        result = await wait_for_discovery_response(request)

    assert result is not None
    assert result.version_info == "test_version_123"
    assert context.data.get("XDS_RESPONSE_SOURCE") == "immediately"
    assert queue.is_empty(), "No render job should have been queued for a cache hit"
