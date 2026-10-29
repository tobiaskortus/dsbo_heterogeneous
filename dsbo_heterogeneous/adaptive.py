import re
import math
import logging
from datetime import timedelta
from inspect import isawaitable
from time import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable, Hashable, Iterable, cast

import dask
import distributed
from tornado.ioloop import IOLoop
from dask.utils import parse_timedelta
from distributed.utils import log_errors
from distributed.protocol import pickle
from distributed.compatibility import PeriodicCallback
from distributed.deploy.adaptive_core import AdaptiveCore
from distributed.deploy.adaptive import AdaptiveStateState
from dsbo_heterogeneous.utils import worker_type_from_name

if TYPE_CHECKING:
    from dsbo_heterogeneous.core import HeterogeneousJobQueueCluster

logger = logging.getLogger(__name__)

class HeterogeneousAdaptiveCore(AdaptiveCore):
    close_counts_groups: defaultdict[str, defaultdict[str, int]]

    def __init__(
        self,
        adapt_config: dict[str, tuple[int | None, int | None]],
        wait_count: int = 3,
    ):
        self.adapt_config = adapt_config
        super().__init__(wait_count=wait_count)
        self.close_counts_groups = defaultdict(lambda: defaultdict(int))

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
            minimum, maximum = self.adapt_config.get(k, (None, None))
            if minimum is not None:
                n = max(n, minimum)
            if maximum is not None:
                n = min(n, maximum)
            targets[k] = cast(int, n)

        return targets

    def _groupby_workers(self, items: Iterable[str]) -> dict[str, set]:
        prefix = f"{self.cluster._name}-"
        groups: defaultdict[str, set] = defaultdict(set)
        for ws in items:
            wtype = worker_type_from_name(ws, prefix) or ws
            groups[wtype].add(ws)
        return groups

    async def workers_to_close(self, wtype: str, workers: Iterable[str], target: int) -> list:
        """Group-scoped close selection: keep ``target`` workers of ``wtype``."""
        return list(workers)[target:]

    def _get_processes(self, wtype: str) -> int:
        spec = self.cluster._spec_workers
        if not spec or wtype not in spec:
            return 1
        return self.cluster._worker_processes(wtype)

    @staticmethod
    def _job_name_of(worker_name: str, processes: int) -> str:
        if processes <= 1:
            return worker_name
        return re.sub(r"-\d+$", "", worker_name)

    def _observed_job_names(self, observed: Iterable[str], processes: int) -> set[str]:
        return {self._job_name_of(w, processes) for w in observed}

    @staticmethod
    def _worker_job_index(name: str) -> int:
        match = re.search(r"-(\d+)(?:-\d+)?$", name)
        return int(match.group(1)) if match else 0

    async def recommendations(self, target: dict[str, int]) -> dict:
        plan_groups = self._groupby_workers(self.plan)
        observed_groups = self._groupby_workers(self.observed)
        requested_groups = self._groupby_workers(self.requested)

        worker_types = (
            requested_groups.keys()
            | plan_groups.keys()
            | observed_groups.keys()
            | target.keys()
        )

        status_groups = defaultdict(dict)

        for worker in worker_types:
            plan = plan_groups.get(worker, set())
            observed = observed_groups.get(worker, set())

            processes = self._get_processes(worker)

            job_target = math.ceil(target.get(worker, 0) / processes)
            worker_target = target.get(worker, 0)

            observed_jobs = self._observed_job_names(observed, processes)
            not_yet_arrived = set(plan) - observed_jobs

            if job_target == len(plan):
                self.close_counts_groups[worker].clear()
                status_groups[worker] = {"status": "same"}
                continue

            if job_target > len(plan):
                self.close_counts_groups[worker].clear()
                status_groups[worker] = {"status": "up", "n": worker_target}
                continue

            to_close: set = set()
            if not_yet_arrived:
                n_to_cancel = max(0, len(plan) - job_target)
                if n_to_cancel:
                    ordered = sorted(not_yet_arrived, key=self._worker_job_index)
                    cancelled = ordered[-n_to_cancel:]
                    to_close.update(cancelled)

            planned_jobs_after_cancel = len(plan) - len(to_close)
            if job_target < planned_jobs_after_cancel:
                obs = observed_groups.get(worker, set())
                if not obs:
                    status_groups[worker] = {"status": "same"}
                    continue

                close_target = job_target * processes
                L = await self.workers_to_close(worker, obs, close_target)
                to_close.update(L)

            close_groups: defaultdict[str, set[str]] = defaultdict(set)
            for w in to_close:
                job = w if w in not_yet_arrived else self._job_name_of(w, processes)
                close_groups[job].add(w)

            firmly_close = set()
            for job, candidates in close_groups.items():
                self.close_counts_groups[worker][job] += 1
                if self.close_counts_groups[worker][job] >= self.wait_count:
                    firmly_close.update(candidates)

            for k in list(self.close_counts_groups[worker]):  # clear out unseen keys
                if k not in close_groups or not close_groups[k].isdisjoint(firmly_close):
                    del self.close_counts_groups[worker][k]

            if firmly_close:
                status_groups[worker] = {"status": "down", "workers": list(firmly_close)}
            else:
                status_groups[worker] = {"status": "same"}

        return status_groups

    async def adapt(self) -> None:
        if self._adapting:  # Semaphore to avoid overlapping adapt calls
            return
        self._adapting = True
        try:
            target = await self.safe_target()
            recommendations = await self.recommendations(target)

            scale_up_recommendations = {}
            scale_down_workers = []

            if any(r["status"] != "same" for r in recommendations.values()):
                self.log.append((time(), {wtype: dict(r) for wtype, r in recommendations.items()}))

            for wtype, recommendation in recommendations.items():
                status = recommendation["status"]
                if status == "same":
                    continue
                if status == "up":
                    scale_up_recommendations[wtype] = recommendation["n"]
                elif status == "down":
                    scale_down_workers.extend(recommendation["workers"])

            if scale_up_recommendations:
                await self.scale_up(scale_up_recommendations)
            if scale_down_workers:
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
        adapt_config: dict[str, tuple[int | None, int | None]] | None = None,
        minimum: int | float = 0,
        maximum: int | float = math.inf,
        wait_count: int | None = None,
        target_duration: str | float | timedelta | None = None,
        worker_key: (Callable[[distributed.scheduler.WorkerState], Hashable] | None) = None,
        **kwargs: Any,
    ):
        self.cluster = cluster
        self.state = "inactive"
        self.worker_key = worker_key

        supported_close_options = {"memory_ratio"}
        unsupported = set(kwargs) - supported_close_options
        if unsupported:
            names = ", ".join(sorted(unsupported))
            raise TypeError(f"Unsupported adaptive option(s): {names}")
        self._workers_to_close_kwargs = dict(kwargs)

        if adapt_config is None:
            raise TypeError("adapt_config is required")
        if minimum != 0 or not math.isinf(maximum):
            raise NotImplementedError(
                "Use adapt_config to define minimum and maximum values per worker type"
            )

        if interval is None:
            interval = dask.config.get("distributed.adaptive.interval")
        if wait_count is None:
            wait_count = cast(int, dask.config.get("distributed.adaptive.wait-count"))
        if target_duration is None:
            target_duration = cast(
                str, dask.config.get("distributed.adaptive.target-duration")
            )
        interval_seconds = parse_timedelta(interval, "seconds")

        super().__init__(
            adapt_config=adapt_config,
            wait_count=wait_count,
        )

        self.interval = interval_seconds
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
            "Adaptive scaling started: configuration=%s",
            self.adapt_config,
        )

    def stop(self, reason: str = "unknown") -> None:
        if self.state in ("inactive", "stopped"):
            return

        if self.state == "running":
            if self.periodic_callback is None:
                raise RuntimeError("periodic_callback is None during stop")
            self.periodic_callback.stop()
        logger.info(
            "Adaptive scaling stopped: configuration=%s. Reason: %s",
            self.adapt_config,
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
        return await super().recommendations(target)

    async def workers_to_close(self, wtype: str, workers: Iterable[str], target: int) -> list[str]:
        worker_list = list(workers)
        if len(worker_list) <= target:
            return []

        all_to_close = await self.scheduler.workers_to_close_group(
            wtype=wtype,
            target=target,
            key=pickle.dumps(self.worker_key) if self.worker_key else None,
            memory_ratio=self._workers_to_close_kwargs.get("memory_ratio"),
        )

        worker_set = set(worker_list)
        return [w for w in all_to_close if w in worker_set]

    @log_errors
    async def scale_down(self, workers):
        if not workers:
            return

        logger.info("Retiring workers %s", workers)
        before = {
            address: str(info["name"])
            for address, info in getattr(self.cluster, "scheduler_info", {}).get("workers", {}).items()
        }
        observed_before = set(self.observed)
        retired = await self.scheduler.retire_workers(
            names=workers,
            remove=True,
            close_workers=True,
        )

        after_info = getattr(self.cluster, "scheduler_info", None)
        after = {
            address: str(info["name"])
            for address, info in (after_info or {}).get("workers", {}).items()
        }
        observed_after = set(self.observed)
       
        untracked = (observed_before | observed_after) - set(before.values()) - set(after.values())
        candidates = set()
        for name in workers:
            if name in self.plan or name in self.requested:
                candidates.add(name)
            else:
                wtype = worker_type_from_name(name, f"{self.cluster._name}-")
                processes = self._get_processes(wtype) if wtype else 1
                candidates.add(self._job_name_of(name, processes))

        safe_jobs = set()
        for job in candidates:
            wtype = worker_type_from_name(job, f"{self.cluster._name}-")
            processes = self._get_processes(wtype) if wtype else 1
            pre_children = {
                address for address, name in before.items()
                if self._job_name_of(name, processes) == job
            }
            if pre_children and after_info is None:
                continue
            live_children = any(
                self._job_name_of(name, processes) == job
                for address, name in after.items() if address not in retired
            )
            unknown_children = any(
                self._job_name_of(name, processes) == job for name in untracked
            )
            if pre_children <= retired.keys() and not live_children and not unknown_children:
                safe_jobs.add(job)

        if safe_jobs:
            f = self.cluster.scale_down(safe_jobs)
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