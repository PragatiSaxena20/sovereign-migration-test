import configparser
import multiprocessing
import os
import tempfile
import warnings
from pathlib import Path
from typing import Callable, cast

import uvicorn

from sovereign import application_logger as log
from sovereign.configuration import SovereignAsgiConfig, SupervisordConfig, config

# noinspection PyArgumentList
asgi_config = SovereignAsgiConfig()
# noinspection PyArgumentList
supervisord_config = SupervisordConfig()


def get_available_cpus() -> int:
    """
    Get the number of available CPUs in a container-aware manner.

    In Docker/EC2 environments, multiprocessing.cpu_count() may return the
    total number of CPUs on the host machine, not the number available to
    the container. This function checks cgroup limits first (both v1 and v2),
    then falls back to os.sched_getaffinity() or multiprocessing.cpu_count().
    """
    try:
        # Try cgroup v2 first (newer systems)
        cgroup_v2_path = Path("/sys/fs/cgroup/cpu.max")
        if cgroup_v2_path.exists():
            content = cgroup_v2_path.read_text().strip()
            quota, period = content.split()
            if quota != "max":
                cpu_limit = int(quota) / int(period)
                return max(1, int(cpu_limit))

        # Try cgroup v1 (older systems / older Docker)
        quota_path = Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        period_path = Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        if quota_path.exists() and period_path.exists():
            quota = int(quota_path.read_text().strip())
            period = int(period_path.read_text().strip())
            if quota > 0 and period > 0:
                cpu_limit = quota / period
                return max(1, int(cpu_limit))
    except (OSError, ValueError, AttributeError) as e:
        log.debug(f"Could not read cgroup CPU limits: {e}")

    try:
        # Use sched_getaffinity if available (Linux)
        # This respects CPU affinity masks set by Docker --cpuset-cpus
        sched_getaffinity = getattr(os, "sched_getaffinity", None)
        if callable(sched_getaffinity):
            affinity = cast(Callable[[int], set[int]], sched_getaffinity)(0)
            return len(affinity)
    except (AttributeError, OSError):
        pass

    # Fall back to multiprocessing.cpu_count()
    return multiprocessing.cpu_count()


def web(supervisor_enabled=True) -> None:
    from sovereign.app import app

    if config.worker_v2_enabled:
        from sovereign.v2.data.utils import get_context_repository_web

        log.info("Pre-warming context")
        get_context_repository_web()
        log.info("Context warmup complete")

    log.debug("Starting web server")

    if not supervisor_enabled:
        uvicorn.run(
            app,
            log_level=asgi_config.log_level,
            access_log=False,
            timeout_keep_alive=asgi_config.keepalive,
            host=asgi_config.host,
            port=asgi_config.port,
            workers=1,  # per managed supervisor proc
        )
    else:
        uvicorn.run(
            app,
            fd=0,
            log_level=asgi_config.log_level,
            access_log=False,
            timeout_keep_alive=asgi_config.keepalive,
            host=asgi_config.host,
            port=asgi_config.port,
            workers=1,  # per managed supervisor proc
        )


def worker():
    if config.worker_v2_enabled:
        from sovereign.v2.worker import Worker

        log.debug("Starting worker v2")
        Worker().start()
    else:
        from sovereign.worker import worker as worker_app

        log.debug("Starting worker")
        uvicorn.run(
            worker_app,
            log_level=asgi_config.log_level,
            access_log=False,
            timeout_keep_alive=asgi_config.keepalive,
            host="127.0.0.1",
            port=9080,
            workers=1,  # per managed supervisor proc
        )


def write_supervisor_conf() -> Path:
    proc_env = {
        "LANG": "en_US.UTF-8",
        "LC_ALL": "en_US.UTF-8",
    }
    base = {
        "autostart": "true",
        "autorestart": "true",
        "stdout_logfile": "/dev/stdout",
        "stdout_logfile_maxbytes": "0",
        "stderr_logfile": "/dev/stderr",
        "stderr_logfile_maxbytes": "0",
        "stopsignal": "QUIT",
        "environment": ",".join(["=".join((k, v)) for k, v in proc_env.items()]),
    }

    conf = configparser.RawConfigParser()
    conf["supervisord"] = supervisord = {
        "nodaemon": str(supervisord_config.nodaemon).lower(),
        "loglevel": supervisord_config.loglevel,
        "pidfile": supervisord_config.pidfile,
        "logfile": supervisord_config.logfile,
        "directory": supervisord_config.directory,
    }

    conf["fcgi-program:web"] = web = {
        **base,
        "socket": f"tcp://{asgi_config.host}:{asgi_config.port}",
        "numprocs": str(asgi_config.workers),
        "process_name": "%(program_name)s-%(process_num)02d",
        "command": "sovereign-web",  # default niceness, higher CPU priority
    }

    if config.worker_v2_enabled:
        available_cpus = get_available_cpus()
        num_workers = max(1, int(available_cpus * config.worker_v2_workers_per_core))
        conf["program:data"] = worker = {
            **base,
            "numprocs": str(num_workers),
            "command": "sovereign-worker",
            "process_name": "sovereign-worker-%(process_num)s",
        }
    else:
        conf["program:data"] = worker = {
            **base,
            "numprocs": "1",
            "command": "nice -n 10 sovereign-worker",  # run worker with reduced CPU priority (higher niceness value)
            "process_name": "sovereign-worker-%(process_num)s",
        }

    if user := asgi_config.user:
        supervisord["user"] = user
        web["user"] = user
        worker["user"] = user

    log.debug("Writing supervisor config")
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        conf.write(f)
        log.debug("Supervisor config written out")
        return Path(f.name)


def main():
    path = write_supervisor_conf()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        from supervisor import supervisord

        log.debug("Starting processes")
        supervisord.main(["-c", path])


if __name__ == "__main__":
    main()
