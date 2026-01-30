
import pickle
import logging
from datetime import timedelta
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Callable, Hashable, Iterable, cast

import dask
import distributed
from tornado.ioloop import IOLoop
from dask.utils import parse_timedelta
from distributed.utils import log_errors
from distributed.deploy.adaptive_core import AdaptiveCore
from distributed.deploy.adaptive import AdaptiveStateState
from distributed.compatibility import PeriodicCallback

if TYPE_CHECKING:
    from dask_heterogeneous.core import HeterogeneousJobQueueCluster

logger = logging.getLogger(__name__)

class HeterogeneousAdaptiveCore(AdaptiveCore):
    def recommendations(self, target):
        print(self.plan, self.requested, self.observed)
        return super().recommendations(target)
    
    

class HeterogeneousAdaptive(HeterogeneousAdaptiveCore):
    interval: float | None
    periodic_callback: PeriodicCallback | None
    state: AdaptiveStateState

    def __init__(
        self,
        cluster: "HeterogeneousJobQueueCluster",
        interval: str | float | timedelta | None = None,
        minimum: int | None = None,
        maximum: int | float | None = None,
        wait_count: int | None = None,
        target_duration: str | float | timedelta | None = None,
        worker_key: (
            Callable[[distributed.scheduler.WorkerState], Hashable] | None
        ) = None,
        **kwargs: Any,
    ):
        self.cluster = cluster
        self.worker_key = worker_key
        self._workers_to_close_kwargs = kwargs

        if interval is None:
            interval = dask.config.get("distributed.adaptive.interval")
        if minimum is None:
            minimum = cast(int, dask.config.get("distributed.adaptive.minimum"))
        if maximum is None:
            maximum = cast(float, dask.config.get("distributed.adaptive.maximum"))
        if wait_count is None:
            wait_count = cast(int, dask.config.get("distributed.adaptive.wait-count"))
        if target_duration is None:
            target_duration = cast(
                str, dask.config.get("distributed.adaptive.target-duration")
            )

        super().__init__(minimum=minimum, maximum=maximum, wait_count=wait_count)

        self.interval = parse_timedelta(interval, "seconds")
        self.periodic_callback = None

        if self.interval and self.cluster:
            import weakref

            self_ref = weakref.ref(self)

            async def _adapt():
                adaptive = self_ref()
                if not adaptive or adaptive.state != "running":
                    return
                if adaptive.cluster.status != distributed.Status.running:
                    adaptive.stop(reason="cluster-not-running")
                    return
                try:
                    await adaptive.adapt()
                except Exception:
                    logger.warning(
                        "Adaptive encountered an error while adapting", exc_info=True
                    )

            self.periodic_callback = PeriodicCallback(_adapt, self.interval * 1000)
            self.state = "starting"
            self.loop.add_callback(self._start)
        else:
            self.state = "inactive"

        self.target_duration = parse_timedelta(target_duration)

    def _start(self) -> None:
        if self.state != "starting":
            return

        assert self.periodic_callback is not None
        self.periodic_callback.start()
        self.state = "running"
        logger.info(
            "Adaptive scaling started: minimum=%s maximum=%s",
            self.minimum,
            self.maximum,
        )

    def stop(self, reason: str = "unknown") -> None:
        if self.state in ("inactive", "stopped"):
            return

        if self.state == "running":
            assert self.periodic_callback is not None
            self.periodic_callback.stop()
        logger.info(
            "Adaptive scaling stopped: minimum=%s maximum=%s. Reason: %s",
            self.minimum,
            self.maximum,
            reason,
        )

        self.periodic_callback = None
        self.state = "stopped"

    @property
    def scheduler(self):
        return self.cluster.scheduler_comm

    @property
    def plan(self):
        return self.cluster.plan

    @property
    def requested(self):
        return self.cluster.requested

    @property
    def observed(self):
        return self.cluster.observed

    async def target(self):
        """
        Determine target number of workers that should exist.

        Notes
        -----
        ``Adaptive.target`` dispatches to Scheduler.adaptive_target(),
        but may be overridden in subclasses.

        Returns
        -------
        Target number of workers

        See Also
        --------
        Scheduler.adaptive_target
        """
        return await self.scheduler.adaptive_target(
            target_duration=self.target_duration
        )

    async def recommendations(self, target: int) -> dict:
        print("Reccomendations", self.plan, self.requested)
        if len(self.plan) != len(self.requested):
            # Ensure that the number of planned and requested workers
            # are in sync before making recommendations.
            await self.cluster

        return await super().recommendations(target)

    async def workers_to_close(self, target: int) -> list[str]:
        """
        Determine which, if any, workers should potentially be removed from
        the cluster.

        Notes
        -----
        ``Adaptive.workers_to_close`` dispatches to Scheduler.workers_to_close(),
        but may be overridden in subclasses.

        Returns
        -------
        List of worker names to close, if any

        See Also
        --------
        Scheduler.workers_to_close
        """
        return await self.scheduler.workers_to_close(
            target=target,
            key=pickle.dumps(self.worker_key) if self.worker_key else None,
            attribute="name",
            **self._workers_to_close_kwargs,
        )

    @log_errors
    async def scale_down(self, workers):
        print(workers)

        if not workers:
            return

        logger.info("Retiring workers %s", workers)
        # Ask scheduler to cleanly retire workers
        await self.scheduler.retire_workers(
            names=workers,
            remove=True,
            close_workers=True,
        )

        # close workers more forcefully
        f = self.cluster.scale_down(workers)
        if isawaitable(f):
            await f

    async def scale_up(self, n):
        print(n)  
        f = self.cluster.scale(n)
        if isawaitable(f):
            await f

    @property
    def loop(self) -> IOLoop:
        """Override Adaptive.loop"""
        if self.cluster:
            return self.cluster.loop  # type: ignore[return-value]
        else:
            return IOLoop.current()

    def __del__(self):
        self.stop(reason="adaptive-deleted")