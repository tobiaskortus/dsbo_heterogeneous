
from collections import defaultdict
import math
import pickle
import logging
import re
from datetime import timedelta
from time import time
from inspect import isawaitable
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

    def __init__(self, adapt_config: dict, wait_count = 3):
        self.adapt_config = adapt_config
        super().__init__(wait_count=wait_count)

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

    async def safe_target(self) -> dict[str, int]:
        targets = await self.target()

        for k, n in list(targets.items()):
            targets[k] = cast(int, min(max(n, self.adapt_config[k][0]), self.adapt_config[k][1]))

        return targets

    def _groupby_workers(self, items: Iterable[str]) -> dict[str, set]:
        prefix = f"{self.cluster._name}-"
        groups: defaultdict[str, set] = defaultdict(set)
        for ws in items:
            #Match names like "cluster-name-0-1" or "cluster-name-0" to worker type "name" 
            wtype = re.sub(r"(-\d+){1,2}$", "", ws.removeprefix(prefix))
            groups[wtype].add(ws)
        return groups

    async def workers_to_close(self, workers, target):
        return list(workers)[target:]

    def _get_processes(self, wtype: str) -> int:
        spec = self.cluster._spec_workers
        if not spec or wtype not in spec:
            return 1
        return spec[wtype]["options"]["processes"]

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Strip the process-index suffix: e.g., 'name-0-1' -> 'name-0'."""
        return re.sub(r"-\d+$", "", name)

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

            processes = self._get_processes(worker)

            plan_workers = len(plan) * processes
            job_target = math.ceil(target.get(worker, 0) / processes)

            worker_target = target.get(worker, 0)

            if worker_target == plan_workers:
                self.close_counts_groups[worker].clear()
                status_groups[worker] = {"status": "same"}
                continue

            if worker_target > plan_workers:
                self.close_counts_groups[worker].clear()
                status_groups[worker] = {"status": "up", "n": worker_target}
                continue

            observed_normalized = {self._normalize_name(w) for w in observed}
            not_yet_arrived = set(requested) - observed_normalized

            to_close: set = set()
            if not_yet_arrived:
                n_to_cancel = len(plan) - job_target
                to_close.update(toolz.take(n_to_cancel, not_yet_arrived))

            if worker_target < plan_workers - len(to_close) * processes:
                obs = observed_groups.get(worker, set())
                if not obs:
                    continue

                L = await self.workers_to_close(obs, worker_target)
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
                if recommendation["status"] != "same":
                    self.log.append((time(), dict(recommendations)))
                    
                status = recommendation.pop("status")
                if status == "same":
                    continue
                if status == "up":
                    scale_up_recommendations[wtype] = recommendation["n"]
                if status == "down":
                    scale_down_workers.extend(recommendation["workers"])
            
            logger.debug("Scale up recommendations: %s", scale_up_recommendations)
            logger.debug("Scale down recommendations: %s", scale_down_workers)
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
        adapt_config: dict[str, int] | None = None,
        wait_count: int | None = None,
        target_duration: str | float | timedelta | None = None,
        worker_key: (
            Callable[[distributed.scheduler.WorkerState], Hashable] | None
        ) = None,
        **kwargs: Any,
    ):
        self.cluster = cluster
        self.worker_key = worker_key

        _valid_wtc_keys = {"memory_ratio", "n", "minimum", "key", "target", "attribute"}
        self._workers_to_close_kwargs = {k: v for k, v in kwargs.items() if k in _valid_wtc_keys}

        if adapt_config is None:
            adapt_config = kwargs["adapt_config"]

        if interval is None:
            interval = dask.config.get("distributed.adaptive.interval")
        if wait_count is None:
            wait_count = cast(int, dask.config.get("distributed.adaptive.wait-count"))
        if target_duration is None:
            target_duration = cast(
                str, dask.config.get("distributed.adaptive.target-duration")
            )

        super().__init__(adapt_config=adapt_config, wait_count=wait_count)

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

        if self.periodic_callback is None:
            raise RuntimeError("periodic_callback is None during _start")
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
            if self.periodic_callback is None:
                raise RuntimeError("periodic_callback is None during stop")
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

    async def recommendations(self, target: dict[str, int]) -> dict:
        if len(self.plan) != len(self.requested):
            # Ensure that the number of planned and requested workers
            # are in sync before making recommendations.
            await self.cluster

        return await super().recommendations(target)

    async def workers_to_close(self, workers, target: int) -> list[str]:
        worker_list = list(workers)
        if len(worker_list) <= target:
            return []

        all_to_close = await self.scheduler.workers_to_close(
            target=target,
            key=pickle.dumps(self.worker_key) if self.worker_key else None,
            attribute="name",
            **self._workers_to_close_kwargs,
        )

        worker_set = set(worker_list)
        return [w for w in all_to_close if w in worker_set]

    @log_errors
    async def scale_down(self, workers):
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
        n_jobs: dict[str, int] = {}
        for wtype, target_workers in n.items():
            processes = self._get_processes(wtype)
            n_jobs[wtype] = math.ceil(target_workers / processes)
        f = self.cluster.scale(n_jobs)
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