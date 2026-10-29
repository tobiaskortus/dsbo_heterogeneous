import asyncio
import os
import signal
import sys

from dask_jobqueue.local import LocalJob
from dsbo_heterogeneous.core import HeterogeneousJobQueueCluster
from dsbo_heterogeneous.scheduler import HeterogeneousScheduler


class WindowsCompatibleLocalJob(LocalJob):
    """LocalJob subclass that works on Windows.

    ``dask_jobqueue.LocalJob._submit_job`` uses ``tornado.process.Subprocess``
    with ``Subprocess.STREAM``, which internally creates a ``PipeIOStream``.
    ``PipeIOStream`` is not supported on Windows (tornado raises
    ``AssertionError`` at construction time).

    This subclass overrides ``_submit_job`` and ``_close_job`` to use
    ``asyncio.create_subprocess_exec`` on Windows, which is fully supported
    via the ``ProactorEventLoop``.  On POSIX systems the base class
    implementation is used unchanged.
    """

    async def _submit_job(self, script_filename: str) -> str:
        if sys.platform != "win32":
            return await super()._submit_job(script_filename)

        import subprocess

        with open(script_filename) as fh:
            text = fh.read().strip().split()

        proc = subprocess.Popen(
            text,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self._win_process = proc

        # Read stderr in a thread executor so we don't block the event loop.
        # SelectorEventLoop (used by tornado/distributed) does not support
        # asyncio.create_subprocess_exec on Windows; subprocess.Popen avoids
        # that constraint entirely.
        def _wait_for_registered(stderr, timeout: int = 60) -> tuple[str | None, str | None]:
            import time
            deadline = time.monotonic() + timeout
            lines: list[str] = []
            while time.monotonic() < deadline:
                raw = stderr.readline()
                if not raw:
                    break
                line = raw.decode(errors="replace")
                lines.append(line)
                if "Registered to:" in line:
                    return str(proc.pid), None
                if "error" in line.lower():
                    return None, "Worker failed to start.\n" + "".join(lines)
            return None, f"Worker did not register within {timeout} s.\n" + "".join(lines)

        loop = asyncio.get_event_loop()
        if proc.stderr is None:
            raise RuntimeError("Subprocess stderr is None; cannot read worker output")
        pid_str, err_msg = await loop.run_in_executor(None, _wait_for_registered, proc.stderr)
        if err_msg is not None:
            proc.kill()
            raise RuntimeError(err_msg)
        return pid_str  # type: ignore[return-value]

    @classmethod
    async def _close_job(cls, job_id: str | None, cancel_command: str) -> None:  # type: ignore[override]
        if job_id is None:
            return
        try:
            pid = int(job_id)
            if sys.platform == "win32":
                os.kill(pid, signal.SIGTERM)
            else:
                os.kill(pid, 9)
        except (ProcessLookupError, PermissionError, OSError, ValueError):
            pass  # Process already gone — nothing to do.


class HeterogeneousLocalCluster(HeterogeneousJobQueueCluster):
    job_cls = WindowsCompatibleLocalJob

    def __init__(self, spec, n_workers=None, loop=None, security=None, shared_temp_directory=None,
                 silence_logs="error", name=None, asynchronous=False, scheduler_options=None,
                 scheduler_cls=HeterogeneousScheduler, **kwargs):
        super().__init__(
            n_workers=n_workers,
            job_cls=self.job_cls,
            spec=spec,
            loop=loop,
            security=security,
            shared_temp_directory=shared_temp_directory,
            silence_logs=silence_logs,
            name=name,
            asynchronous=asynchronous,
            scheduler_options=scheduler_options,
            scheduler_cls=scheduler_cls,
            **kwargs,
        )