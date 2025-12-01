"""
Tests for rendering module, including RenderJob timeout and thread configuration.
"""
import os
import time
from unittest.mock import Mock, patch, MagicMock
from multiprocessing import Process, Pipe

import pytest

from sovereign.rendering import RenderJob, POOL, asgi_config
from sovereign.types import DiscoveryRequest
from sovereign.configuration import SovereignAsgiConfig


class TestThreadPoolConfiguration:
    """Test that SOVEREIGN_THREADS configuration is respected."""

    def test_pool_uses_configured_threads(self):
        """Verify that POOL uses asgi_config.threads instead of cpu_count()."""
        # The POOL should be initialized with asgi_config.threads
        # We can't easily test the exact value without mocking, but we can verify
        # it's using the config object
        assert hasattr(POOL, "_max_workers")
        # The max_workers should match the configured threads
        assert POOL._max_workers == asgi_config.threads

    @patch.dict(os.environ, {"SOVEREIGN_THREADS": "8"})
    def test_pool_respects_environment_variable(self):
        """Verify that changing SOVEREIGN_THREADS affects the pool."""
        # Reload the module to pick up new env var
        import importlib
        import sovereign.rendering

        # Create a new config instance with the new env var
        new_config = SovereignAsgiConfig()
        # The POOL is created at module import time, so we need to check
        # that the asgi_config instance reads from the environment
        # In practice, the POOL is created once at import, so this test
        # verifies the configuration is read correctly
        assert new_config.threads == 8


class TestRenderJobTimeout:
    """Test that RenderJob respects SOVEREIGN_WORKER_TIMEOUT configuration."""

    def test_timeout_uses_configured_value(self):
        """Verify that the timeout uses asgi_config.worker_timeout."""
        # Create a mock request
        request = Mock(spec=DiscoveryRequest)
        request.template = Mock()
        request.template.generate = Mock(return_value={"resources": []})
        request.template.is_python_source = False
        request.resource_type = "listeners"
        request.api_version = "v3"
        request.resources = []
        request.desired_controlplane = "test"
        request.node = Mock()

        job = RenderJob(id="test-id", request=request, context={})

        # Mock the process to verify timeout is used
        with patch("sovereign.rendering.Process") as mock_process_class:
            mock_proc = MagicMock()
            mock_proc.is_alive.return_value = False
            mock_proc.start = Mock()
            mock_process_class.return_value = mock_proc

            # Mock Pipe
            with patch("sovereign.rendering.Pipe") as mock_pipe:
                mock_rx = MagicMock()
                mock_rx.poll.return_value = False
                mock_tx = MagicMock()
                mock_pipe.return_value = (mock_rx, mock_tx)

                # Run the job
                job._run()

                # Verify that join was called with the configured timeout
                expected_timeout = asgi_config.worker_timeout
                mock_proc.join.assert_called_with(timeout=expected_timeout)

    @patch.dict(os.environ, {"SOVEREIGN_WORKER_TIMEOUT": "120"})
    def test_timeout_respects_environment_variable(self):
        """Verify that changing SOVEREIGN_WORKER_TIMEOUT affects the timeout."""
        # Create a new config instance with the new env var
        new_config = SovereignAsgiConfig()
        assert new_config.worker_timeout == 120


class TestRenderJobProcessCleanup:
    """Test that RenderJob properly cleans up processes that exceed timeout."""

    def test_process_terminated_on_timeout(self):
        """Verify that a process is terminated if it exceeds the timeout."""
        request = Mock(spec=DiscoveryRequest)
        request.template = Mock()
        request.template.generate = Mock(return_value={"resources": []})
        request.template.is_python_source = False
        request.resource_type = "listeners"
        request.api_version = "v3"
        request.resources = []
        request.desired_controlplane = "test"
        request.node = Mock()

        job = RenderJob(id="test-id", request=request, context={})

        with patch("sovereign.rendering.Process") as mock_process_class:
            mock_proc = MagicMock()
            mock_proc.pid = 12345
            mock_proc.is_alive.side_effect = [True, False]  # Alive after timeout, then dead
            mock_proc.start = Mock()
            mock_process_class.return_value = mock_proc

            with patch("sovereign.rendering.Pipe") as mock_pipe:
                mock_rx = MagicMock()
                mock_rx.poll.return_value = False
                mock_tx = MagicMock()
                mock_pipe.return_value = (mock_rx, mock_tx)

                with patch("sovereign.rendering.log") as mock_log:
                    job._run()

                    # Verify terminate was called
                    mock_proc.terminate.assert_called_once()
                    # Verify join was called after terminate
                    assert mock_proc.join.call_count >= 2  # Once for timeout, once after terminate
                    # Verify warning was logged
                    mock_log.warning.assert_called()

    def test_process_killed_if_terminate_fails(self):
        """Verify that a process is force-killed if terminate doesn't work."""
        request = Mock(spec=DiscoveryRequest)
        request.template = Mock()
        request.template.generate = Mock(return_value={"resources": []})
        request.template.is_python_source = False
        request.resource_type = "listeners"
        request.api_version = "v3"
        request.resources = []
        request.desired_controlplane = "test"
        request.node = Mock()

        job = RenderJob(id="test-id", request=request, context={})

        with patch("sovereign.rendering.Process") as mock_process_class:
            mock_proc = MagicMock()
            mock_proc.pid = 12345
            # Process stays alive even after terminate
            mock_proc.is_alive.return_value = True
            mock_proc.start = Mock()
            mock_process_class.return_value = mock_proc

            with patch("sovereign.rendering.Pipe") as mock_pipe:
                mock_rx = MagicMock()
                mock_rx.poll.return_value = False
                mock_tx = MagicMock()
                mock_pipe.return_value = (mock_rx, mock_tx)

                with patch("sovereign.rendering.log") as mock_log:
                    job._run()

                    # Verify terminate was called
                    mock_proc.terminate.assert_called_once()
                    # Verify kill was called after terminate failed
                    mock_proc.kill.assert_called_once()
                    # Verify multiple warnings were logged
                    assert mock_log.warning.call_count >= 2

    def test_process_not_terminated_if_completes_in_time(self):
        """Verify that a process is not terminated if it completes within timeout."""
        request = Mock(spec=DiscoveryRequest)
        request.template = Mock()
        request.template.generate = Mock(return_value={"resources": []})
        request.template.is_python_source = False
        request.resource_type = "listeners"
        request.api_version = "v3"
        request.resources = []
        request.desired_controlplane = "test"
        request.node = Mock()

        job = RenderJob(id="test-id", request=request, context={})

        with patch("sovereign.rendering.Process") as mock_process_class:
            mock_proc = MagicMock()
            mock_proc.is_alive.return_value = False  # Process completes normally
            mock_proc.start = Mock()
            mock_process_class.return_value = mock_proc

            with patch("sovereign.rendering.Pipe") as mock_pipe:
                mock_rx = MagicMock()
                mock_rx.poll.return_value = False
                mock_tx = MagicMock()
                mock_pipe.return_value = (mock_rx, mock_tx)

                job._run()

                # Verify terminate was NOT called
                mock_proc.terminate.assert_not_called()
                # Verify kill was NOT called
                mock_proc.kill.assert_not_called()

