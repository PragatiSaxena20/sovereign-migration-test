"""
Sovereign Cache Module

This module provides an extensible cache backend system that allows clients
to configure their own remote cache backends through entry points.
"""

import asyncio
import os
from typing import Any
from typing_extensions import final

import requests

from sovereign import WORKER_URL, stats, application_logger as log
from sovereign.types import DiscoveryRequest, RegisterClientRequest
from sovereign.configuration import config
from sovereign.cache.types import Entry, CacheResult
from sovereign.cache.backends import CacheBackend, get_backend
from sovereign.cache.filesystem import FilesystemCache


CACHE_READ_TIMEOUT = config.cache.read_timeout


class CacheManagerBase:
    def __init__(self) -> None:
        self.local: FilesystemCache = FilesystemCache()
        self.remote: CacheBackend | None = get_backend()
        if self.remote is None:
            log.info("Cache initialized with filesystem backend only")
        else:
            log.info("Cache initialized with filesystem and remote backends")


@final
class CacheReader(CacheManagerBase):
    def try_read(self, key: str) -> CacheResult | None:
        # Try filesystem first
        if value := self.local.get(key):
            stats.increment("cache.fs.hit")
            return CacheResult(value=value, from_remote=False)
        stats.increment("cache.fs.miss")

        # Fallback to remote cache if available
        if self.remote:
            try:
                if value := self.remote.get(key):
                    ret = CacheResult(value=value, from_remote=True)
                    # Write back to filesystem
                    self.local.set(key, value)
                    stats.increment("cache.remote.hit")
                    return ret
            except Exception as e:
                log.warning(f"Failed to read from remote cache: {e}")
                stats.increment("cache.remote.error")
            stats.increment("cache.remote.miss")
            log.debug(f"Failed to read from either cache for {key}")
        return None


    # noinspection PyShadowingBuiltins
    def get(self, req: DiscoveryRequest) -> Entry | None:
        id = client_id(req)
        if result := self.try_read(id):
            if result.from_remote:
                _ = self.register_over_http(req)
            return result.value
        return None

    @stats.timed("cache.read_ms")
    async def blocking_read(
        self, req: DiscoveryRequest, timeout_s=CACHE_READ_TIMEOUT, poll_interval_s=0.5
    ) -> Entry | None:
        metric = "client.registration"
        cache_key = client_id(req)
        log.info(f"[blocking_read] Starting blocking read for {cache_key}, resource_type={req.resource_type}, timeout={timeout_s}s")

        if entry := self.get(req):
            log.info(f"[blocking_read] Cache hit on first check for {cache_key}")
            return entry

        log.info(f"[blocking_read] Cache miss for {cache_key}, registering client and waiting for entry")
        registered = False
        start = asyncio.get_event_loop().time()
        attempt = 1
        poll_count = 0
        while (asyncio.get_event_loop().time() - start) < timeout_s:
            elapsed = asyncio.get_event_loop().time() - start
            if not registered:
                try:
                    log.debug(f"[blocking_read] Attempting registration for {cache_key} (attempt {attempt}, elapsed {elapsed:.2f}s)")
                    if self.register_over_http(req):
                        stats.increment(metric, tags=["status:registered"])
                        registered = True
                        log.info(f"[blocking_read] Client {cache_key} registered successfully after {elapsed:.2f}s")
                    else:
                        stats.increment(metric, tags=["status:ratelimited"])
                        wait_time = min(attempt, CACHE_READ_TIMEOUT)
                        log.debug(f"[blocking_read] Registration rate limited for {cache_key}, waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                        attempt *= 2
                except Exception as e:
                    stats.increment(metric, tags=["status:failed"])
                    log.exception(f"[blocking_read] Registration failed for {cache_key}: {e}")

            poll_count += 1
            if entry := self.get(req):
                log.info(f"[blocking_read] Entry found for {cache_key} after {poll_count} polls, elapsed {elapsed:.2f}s")
                return entry

            if poll_count % 10 == 0:
                log.debug(f"[blocking_read] Still waiting for {cache_key}, poll_count={poll_count}, elapsed={elapsed:.2f}s, registered={registered}")

            await asyncio.sleep(poll_interval_s)

        total_elapsed = asyncio.get_event_loop().time() - start
        log.warning(f"[blocking_read] Timeout waiting for {cache_key} after {total_elapsed:.2f}s, {poll_count} polls, registered={registered}")
        return None

    @staticmethod
    def register_over_http(req: DiscoveryRequest) -> bool:
        registration = RegisterClientRequest(request=req)
        cache_key = client_id(req)
        log.debug(f"[register_over_http] Sending registration to worker for {cache_key}")
        try:
            response = requests.put(
                f"{WORKER_URL}/client",
                json=registration.model_dump(),
                timeout=3,
            )
            match response.status_code:
                case 200 | 202:
                    log.debug(f"[register_over_http] Worker responded OK ({response.status_code}) to registration for {cache_key}")
                    return True
                case code:
                    log.warning(f"[register_over_http] Worker responded with {code} to registration for {cache_key}")
        except Exception as e:
            log.exception(f"[register_over_http] Error while registering client {cache_key}: {e}")
        return False


@final
class CacheWriter(CacheManagerBase):
    def set(
        self, key: str, value: Entry, timeout: int | None = None
    ) -> tuple[bool, list[tuple[str, str]]]:
        msg = []
        cached = False

        log.debug(f"[CacheWriter.set] Writing cache entry for {key}, version={value.version}, len={value.len}")

        if os.getenv("CACHE_DISABLED") == "true":
            log.debug(f"[CacheWriter.set] Cache disabled, skipping write for {key}")
            return cached, msg

        try:
            self.local.set(key, value, timeout)
            stats.increment("cache.fs.write.success")
            cached = True
            log.debug(f"[CacheWriter.set] Successfully wrote to filesystem cache for {key}")
        except Exception as e:
            log.warning(f"[CacheWriter.set] Failed to write to filesystem cache for {key}: {e}")
            msg.append(("warning", f"Failed to write to filesystem cache: {e}"))
            stats.increment("cache.fs.write.error")
        if self.remote:
            try:
                self.remote.set(key, value, timeout)
                stats.increment("cache.remote.write.success")
                cached = True
                log.debug(f"[CacheWriter.set] Successfully wrote to remote cache for {key}")
            except Exception as e:
                log.warning(f"[CacheWriter.set] Failed to write to remote cache for {key}: {e}")
                msg.append(("warning", f"Failed to write to remote cache: {e}"))
                stats.increment("cache.remote.write.error")

        log.info(f"[CacheWriter.set] Cache write complete for {key}, cached={cached}")
        return cached, msg


    # noinspection PyShadowingBuiltins
    def register(self, req: DiscoveryRequest):
        id = client_id(req)
        log.info(f"[CacheWriter.register] Registering client {id}, resource_type={req.resource_type}")
        self.local.register(id, req)
        log.debug(f"[CacheWriter.register] Client {id} registered in local cache")
        return id, req


    # noinspection PyShadowingBuiltins
    def registered(self, req: DiscoveryRequest) -> bool:
        ret = False
        id = client_id(req)
        if value := self.local.registered(id):
            ret = value
        log.debug(f"Client {id} registered={ret}")
        return ret

    def get_registered_clients(self) -> list[tuple[str, Any]]:
        if value := self.local.get_registered_clients():
            return value
        return []


def client_id(req: DiscoveryRequest) -> str:
    return req.cache_key(config.cache.hash_rules)
