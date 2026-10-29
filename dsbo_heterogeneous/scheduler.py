import dask
import logging
import math
import re
from collections import defaultdict
from tlz import concat, take
from dask.utils import parse_timedelta
from distributed import Scheduler
from distributed.scheduler import WorkerState
from distributed.protocol import pickle

from dsbo_heterogeneous.utils import job_name_from_worker, worker_type_from_name

logger = logging.getLogger(__name__)


class HeterogeneousScheduler(Scheduler):
    def __init__(self, *args, spec_workers=None, worker_name_prefix=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._spec_workers = spec_workers or {}
        self._worker_name_prefix = worker_name_prefix
        self.handlers["workers_to_close_group"] = self.workers_to_close_group

    def _worker_type(self, ws: WorkerState) -> str:
        prefix = getattr(self, "_worker_name_prefix", None)
        if prefix:
            wtype = worker_type_from_name(ws.name, prefix)
            if wtype is not None:
                return wtype
        parts = ws.name.split("-")
        return parts[1] if len(parts) > 1 else ws.name

    def _apply_annotations(self, tasks, annotations_by_type, global_annotations=None):
        """``HeterogeneousScheduler`` requires every task to declare a ``resources``
        mapping (either via ``client.submit(..., resources=...)`` or a surrounding
        ``dask.annotate(resources=...)`` block) so it can be routed to a worker type.
        """
        keys = super()._apply_annotations(
            tasks,
            annotations_by_type,
            global_annotations=global_annotations,
        )

        for ts in tasks: self._task_worker_type(ts)
        return keys

    def _task_worker_type(self, ts) -> str:
        """Return the worker type a task is routed to."""
        restrictions = getattr(ts, "resource_restrictions", None)
        if not restrictions:
            key = getattr(ts, "key", None)
            raise ValueError(
                "Task {!r} has no 'resources' mapping; every task in a "
                "heterogeneous cluster must declare resources=... (or be "
                "submitted inside a dask.annotate(resources=...) block) so it "
                "can be routed to a worker type.".format(key)
            )
        if len(restrictions) != 1:
            raise ValueError(
                "Task {!r} declares multiple resources {}; a heterogeneous "
                "cluster expects exactly one resource naming the worker type "
                "the task is routed to.".format(
                    getattr(ts, "key", None), set(restrictions)
                )
            )
        wtype, = restrictions
        known = getattr(self, "_spec_workers", None)
        if known and wtype not in known:
            raise ValueError(
                "Task {!r} routes to unknown worker type {!r}; expected one of "
                "{}.".format(getattr(ts, "key", None), wtype, sorted(known))
            )
        return wtype

    def _calc_per_worker_occupancy(self, network_occ: float) -> dict[str, float]:
        res_dict = {}
        occ_dict = {}

        for ts in self.tasks.values():
            if not ts.state == "processing":
                continue

            prefix_name = ts.prefix.name
            self.task_prefixes[prefix_name]
            duration = self._get_prefix_duration(self.task_prefixes[prefix_name])
            task_worker = self._task_worker_type(ts)
            res_dict[task_worker] = res_dict.get(task_worker, 0) + duration
        
        for worker_type, res in res_dict.items():
            occ_dict[worker_type] = res + network_occ / self.bandwidth
            if self.validate:
                assert occ_dict[worker_type] >= 0, (occ_dict[worker_type], res, network_occ, self.bandwidth)
            occ_dict[worker_type] = max(occ_dict[worker_type], 0)

        return occ_dict
    
    @property
    def total_per_worker_occupancy(self) -> dict[str, float]:
        return self._calc_per_worker_occupancy(self._network_occ_global)
    
    @property
    def worker_groups(self) -> dict[str, dict[str, WorkerState]]:
        groups = defaultdict(dict)

        for addr, ws in self.workers.items():
            wtype = self._worker_type(ws)
            groups[wtype][addr] = ws

        return groups
    
    @property
    def queue_groups(self) -> dict[str, list]:
        queued_by_type: dict[str, list] = defaultdict(list)
        for ts in self.queued:
            wtype = self._task_worker_type(ts)
            queued_by_type[wtype].append(ts)
        return queued_by_type

    @property
    def unrunnable_groups(self) -> dict[str, list]:
        unrunnable_by_type: dict[str, list] = defaultdict(list)
        for ts in self.unrunnable.keys():
            wtype = self._task_worker_type(ts)
            unrunnable_by_type[wtype].append(ts)
        return unrunnable_by_type

    def _worker_processes(self, wtype: str) -> int:
        return self._spec_workers[wtype]["options"]["processes"]

    @staticmethod
    def _parent_job_name(name: str, processes: int) -> str:
        return job_name_from_worker(name, processes)

    def workers_to_close_group(
        self,
        wtype: str,
        target: int | None = None,
        minimum: int | None = None,
        key: bytes | None = None,
        memory_ratio: float | None = None,
    ) -> list[str]:
        """Return worker names of whole ``wtype`` jobs that can be closed.
        This is the group-scoped counterpart of :meth:`Scheduler.workers_to_close`.
        """
        workers = [ws for ws in self.workers.values() if self._worker_type(ws) == wtype]
        if not workers:
            return []

        if target is None and memory_ratio is None:
            memory_ratio = 2

        processes = self._worker_processes(wtype)
        jobs: dict[str, list[WorkerState]] = defaultdict(list)
        for ws in workers:
            jobs[self._parent_job_name(ws.name, processes)].append(ws)

        if isinstance(key, bytes):
            key = pickle.loads(key)

        if key is None:
            clusters: dict[object, list[str]] = {job: [job] for job in jobs}
        else:
            grouped: dict[object, list[str]] = defaultdict(list)
            for job, job_workers in jobs.items():
                grouped[key(job_workers[0])].append(job)
            clusters = grouped

        def cluster_workers(job_names: list[str]) -> list[WorkerState]:
            return [ws for name in job_names for ws in jobs[name]]

        def is_closable(job_names: list[str]) -> bool:
            cluster_ws = cluster_workers(job_names)
            if any(ws.long_running or ws.processing for ws in cluster_ws):
                return False
            valid = list(cluster_ws)
            for plugin in list(self.plugins.values()):
                valid = plugin.valid_workers_downscaling(self, valid)
            return len(valid) == len(cluster_ws)

        def nbytes_of(job_names: list[str]) -> int:
            return sum(ws.nbytes for ws in cluster_workers(job_names))

        closable = sorted(
            (names for names in clusters.values() if is_closable(names)),
            key=nbytes_of,
        )

        to_close: list[str] = []
        n_remain = sum(len(job_workers) for job_workers in jobs.values())
        limit = sum(ws.memory_limit for ws in workers)
        used = sum(ws.nbytes for ws in workers)
        for names in closable:
            group = cluster_workers(names)
            size = len(group)
            if minimum is not None and n_remain - size < minimum:
                continue

            remaining_limit = limit - sum(ws.memory_limit for ws in group)
            target_allows = target is not None and n_remain - size >= target
            memory_allows = memory_ratio is not None and remaining_limit >= memory_ratio * used

            if not (target_allows or memory_allows):
                continue
            
            to_close.extend(names)
            n_remain -= size
            limit = remaining_limit

        result = [ws.name for name in to_close for ws in jobs[name]]
        if result:
            logger.debug("Suggest closing workers of type %s: %s", wtype, result)
        return result

    def adaptive_target(self, worker_types: list[str], target_duration: float | None = None) -> dict[str, int]:
        if not set(self.worker_groups.keys()).issubset(set(worker_types)):
            unknown = set(self.worker_groups.keys()) - set(worker_types)
            raise ValueError(
                "Unknown worker types found in scheduler: {}. "
                "Expected worker_types={}; scheduler_groups={}; _spec_workers={}.".format(
                    unknown, list(worker_types), list(self.worker_groups.keys()), list(getattr(self, "_spec_workers", {}) or {})
                )
            )

        target_duration = target_duration or dask.config.get("distributed.adaptive.target-duration")
        target_duration = parse_timedelta(target_duration)

        targets = {wtype: 0 for wtype in worker_types}
        worker_groups = self.worker_groups
        queue_groups = self.queue_groups
        unrunnable_groups = self.unrunnable_groups
        total_occupancy_groups = self.total_per_worker_occupancy

        for worker_type in worker_types:
            workers = worker_groups.get(worker_type, {})
            queued_occupancy = 0.0
            total_occupancy = total_occupancy_groups.get(worker_type, 0.0)

            tasks_ready = len(queue_groups.get(worker_type, [])) + \
                          len(unrunnable_groups.get(worker_type, []))
            
            queued = take(100, concat([queue_groups.get(worker_type, []), 
                                       unrunnable_groups.get(worker_type, [])]))
            
            for ts in queued:
                queued_occupancy += self._get_prefix_duration(ts.prefix)

            if tasks_ready > 100:
                queued_occupancy *= tasks_ready / 100

            cpu = math.ceil((total_occupancy + queued_occupancy) / target_duration)

            for ws in workers.values():
                if tasks_ready > cpu:
                    break
                tasks_ready += len(ws.processing)
            else:
                cpu = min(tasks_ready, cpu)

            if workers:
                capacity = 0
                for ws in workers.values():
                    resource = int(ws.resources.get(worker_type, 0))
                    capacity += min(ws.nthreads, resource) if resource else ws.nthreads

                if capacity:
                    cpu = math.ceil(cpu / capacity * len(workers))

            if ((queue_groups.get(worker_type, None) or unrunnable_groups.get(worker_type, None)) 
                and not workers):
                cpu = max(1, cpu)

            limit = sum(ws.memory_limit for ws in workers.values())
            used = sum(ws.nbytes for ws in workers.values())
            memory = 0
            if used > 0.6 * limit and limit > 0:
                memory = 2 * len(workers)

            target = max(memory, cpu)
            busy = sum(1 for ws in workers.values() if ws.processing or ws.long_running)
            if target < busy:
                target = busy

            workers_are_undersubscribed = any(
                0 < ws.resources.get(worker_type, 0) < ws.nthreads
                for ws in workers.values()
            )

            if workers_are_undersubscribed:
                demand = (
                    len(queue_groups.get(worker_type, ()))
                    + len(unrunnable_groups.get(worker_type, ()))
                    + sum(len(ws.processing) for ws in workers.values())
                )
                target = max(target, math.ceil(demand * len(workers) / capacity))

            if target < len(workers):
                closable = self.workers_to_close_group(worker_type)
                target = max(target, len(workers) - len(closable))

            targets[worker_type] = target

        return targets
