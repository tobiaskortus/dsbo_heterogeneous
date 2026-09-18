import dask
import math
from collections import defaultdict
from tlz import concat, take
from dask.utils import parse_timedelta
from distributed import Scheduler
from distributed.scheduler import WorkerState

class HeterogeneousScheduler(Scheduler):
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
            wtype = ws.name.split("-")[1]
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
        closable = None

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

            for ws in worker_groups.get(worker_type, {}).values():
                if tasks_ready > cpu:
                    break
                tasks_ready += len(ws.processing)
            else:
                cpu = min(tasks_ready, cpu)

            if worker_groups.get(worker_type, None):
                capacity = sum(
                    min(ws.nthreads, int(ws.resources.get(worker_type, 0)))
                    for ws in worker_groups[worker_type].values()
                )

                if capacity:
                    cpu = math.ceil(cpu / capacity * len(worker_groups[worker_type]))

            if ((queue_groups.get(worker_type, None) or unrunnable_groups.get(worker_type, None)) 
                and not workers):
                cpu = max(1, cpu)

            limit = sum(ws.memory_limit for ws in workers.values())
            used = sum(ws.nbytes for ws in workers.values())
            memory = 0
            if used > 0.6 * limit and limit > 0:
                memory = 2 * len(workers)
            target = max(memory, cpu)
            if target < len(workers):
                if closable is None:
                    closable = set(self.workers_to_close(attribute="name"))
                target = len(workers) - sum(ws.name in closable for ws in workers.values())
            targets[worker_type] = target

        return targets

