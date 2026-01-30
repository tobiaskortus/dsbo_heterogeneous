
from collections import defaultdict
import pickle
import logging
from datetime import time, timedelta
from inspect import isawaitable
import stat
from typing import TYPE_CHECKING, Any, Callable, Hashable, Iterable, cast

import dask
import distributed
import toolz
from tornado.ioloop import IOLoop
from dask.utils import parse_timedelta
from distributed.utils import log_errors
from distributed.scheduler import WorkerState
from distributed.deploy.adaptive_core import AdaptiveCore
from distributed.deploy.adaptive import AdaptiveStateState
from distributed.compatibility import PeriodicCallback

if TYPE_CHECKING:
    from dask_heterogeneous.core import HeterogeneousJobQueueCluster

logger = logging.getLogger(__name__)

class HeterogeneousAdaptiveCore(AdaptiveCore):
    close_counts_groups: defaultdict[str, defaultdict[WorkerState, int]] | None
    close_counts: dict[WorkerState, int] | None

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

    async def safe_target(self) -> int:
        targets = await self.target()

        for k, n in list(targets.items()):
            targets[k] = cast(int, min(max(n, self.minimum), self.maximum))

        return targets

    def _groupby_workers(self, items: Iterable[WorkerState]) -> dict[str, set[WorkerState]]:
        groups: defaultdict[str, set[WorkerState]] = defaultdict(set)
        for ws in items:
            wtype = ws.rsplit("-", 2)[1]
            groups[wtype].add(ws)
        return groups

    async def workers_to_close(self, workers, target):
        return list(workers)[:target]

    async def recommendations(self, target: dict[str, int]) -> dict:
        if not hasattr(self, "close_counts_groups") or self.close_counts_groups is None:
            self.close_counts_groups = defaultdict(lambda: defaultdict(int))
        if not hasattr(self, "close_counts"):
            self.close_counts = {}

        plan_groups      = self._groupby_workers(self.plan)
        observed_groups  = self._groupby_workers(self.observed)
        requested_groups = self._groupby_workers(self.requested)

        workers = (requested_groups.keys() | plan_groups.keys() |  
                   observed_groups.keys() | target.keys())
        
        for worker in workers:
            if worker not in self.close_counts_groups:
                self.close_counts_groups[worker] = defaultdict(int)

        status_groups = defaultdict(dict)

        for worker in workers:
            plan = plan_groups.get(worker, set())
            observed = observed_groups.get(worker, set())
            requested = requested_groups.get(worker, set())

            worker_target = target.get(worker, 0)

            if worker_target == len(plan):
                self.close_counts_groups[worker].clear()
                status_groups[worker] = {"status": "same"}
                continue

            if worker_target > len(plan):
                add_n = max(0, worker_target - len(plan))
                self.close_counts_groups[worker].clear()
                status_groups[worker] = {"status": "up", "n": add_n}
                continue

            not_yet_arrived = requested - observed
            to_close = set()
            if not_yet_arrived:
                to_close.update(toolz.take(len(plan) - worker_target, not_yet_arrived))

            if worker_target < len(plan) - len(to_close):
                 # Call the core implementation explicitly (accepts `workers, target`)
                L = await HeterogeneousAdaptiveCore.workers_to_close(self, observed_groups[worker], worker_target)
                to_close.update(L)

            firmly_close = set()
            for w in to_close:
                self.close_counts_groups[worker][w] += 1
                if self.close_counts_groups[worker][w] >= self.wait_count:
                    firmly_close.add(w)

            for k in list(self.close_counts_groups[worker]):  # clear out unseen keys
                if k in firmly_close or k not in to_close:
                    del self.close_counts_groups[worker][k]

            if firmly_close:
                status_groups[worker] = {"status": "down", "workers": list(firmly_close)}
                continue
            else:
                status_groups[worker] = {"status": "same"}
                continue
        
        self.close_counts = {w: self.close_counts.get(w, 0) for grp in self.close_counts_groups.values() for w in grp}
        return status_groups
            
    
    async def adapt(self) -> None:
        if self._adapting:  # Semaphore to avoid overlapping adapt calls
            return
        self._adapting = True
        status = None

        try:
            target = await self.safe_target()
            recommendations = await self.recommendations(target)

            scale_up_recommendations = {}
            scale_down_workers = []

            for wtype, recommendation in recommendations.items():
                #TODO: handle multiple worker types better
                if recommendation["status"] != "same":
                    self.log.append((time(), dict(recommendations)))
                    
                status = recommendation.pop("status")
                if status == "same":
                    continue
                if status == "up":
                    scale_up_recommendations[wtype] = recommendation["n"]
                if status == "down":
                    scale_down_workers.extend(recommendation["workers"])
            
            print("Scale_up:", scale_up_recommendations)
            print("Scale_down:", scale_down_workers)
            await self.scale_up(scale_up_recommendations)
            await self.scale_down(scale_down_workers)
        finally:
            self._adapting = False  
    

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

    async def target(self):
        worker_types = self.cluster._spec_workers.keys()
        return await self.scheduler.adaptive_target(
            worker_types=list(worker_types),
            target_duration=self.target_duration
        )

    async def recommendations(self, target: int) -> dict:
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
        print("Scaling down workers:", workers)
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

    async def scale_up(self, n: dict[str, int]):
        print("Scaling up workers:", n)
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