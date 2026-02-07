from asyncio import tasks
import queue
import dask
import math
import logging
from tlz import concat, take
from distributed import Scheduler
from collections import defaultdict
from dask.utils import parse_timedelta
from distributed.scheduler import WorkerState

class HeterogeneousScheduler(Scheduler):
    def _calc_per_worker_occupancy(self, network_occ: float) -> float:
        res_dict = {}
        occ_dict = {}

        for ts in self.tasks.values():
            if not ts.state == "processing":
                continue

            prefix_name = ts.prefix.name
            self.task_prefixes[prefix_name]
            duration = self._get_prefix_duration(self.task_prefixes[prefix_name])
            task_worker = next(iter(ts.resource_restrictions.keys())) 
            res_dict[task_worker] = res_dict.get(task_worker, 0) + duration
        
        for worker_type, res in res_dict.items():
            occ_dict[worker_type] = res + network_occ / self.bandwidth
            if self.validate:
                assert occ_dict[worker_type] >= 0, (occ_dict[worker_type], res, network_occ, self.bandwidth)
            occ_dict[worker_type] = max(occ_dict[worker_type], 0)

        return occ_dict
    
    @property
    def total_per_worker_occupancy(self) -> dict[str, float]:
        #TODO can I use self._network_occ_global here?
        return self._calc_per_worker_occupancy(self._network_occ_global)
    
    @property
    def worker_groups(self) -> dict[str, dict[str, WorkerState]]:
        groups = defaultdict(dict)

        for addr, ws in self.workers.items():
            wtype = ws.name.rsplit("-", 2)[1]
            groups[wtype][addr] = ws

        return groups
    
    @property
    def queue_groups(self) -> dict[str, list]:
        queued_by_type: dict[str, list] = defaultdict(list)
        for ts in self.queued:
            wtype = next(iter(ts.resource_restrictions.keys()))
            queued_by_type[wtype].append(ts)
        return queued_by_type

    @property
    def unrunnable_groups(self) -> dict[str, list]:
        unrunnable_by_type: dict[str, list] = defaultdict(list)
        for ts in self.unrunnable.keys():
            wtype = next(iter(ts.resource_restrictions.keys()))
            unrunnable_by_type[wtype].append(ts)
        return unrunnable_by_type

    def adaptive_target(self, worker_types: list[str], target_duration: float | None = None) -> dict[str, int]:
        assert set(self.worker_groups.keys()).issubset(set(worker_types)), "Unknown worker types found in scheduler"

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

            for ws in worker_groups.get(worker_type, {}).values():
                if tasks_ready > cpu:
                    break
                tasks_ready += len(ws.processing)
            else:
                cpu = min(tasks_ready, cpu)

            if worker_groups.get(worker_type, None):
                nthreads = sum(ws.nthreads for ws in worker_groups[worker_type].values())
                cpu = math.ceil(cpu / nthreads * len(worker_groups[worker_type]))

            if ((queue_groups.get(worker_type, None) or unrunnable_groups.get(worker_type, None)) 
                and not workers):
                cpu = max(1, cpu)

            limit = sum(ws.memory_limit for ws in workers.values())
            used = sum(ws.nbytes for ws in workers.values())
            memory = 0
            if used > 0.6 * limit and limit > 0:
                memory = 2 * len(workers)
            target = max(memory, cpu)
            if target >= len(workers):
                targets[worker_type] = target
            else:
                to_close = self.workers_to_close()
                targets[worker_type] = len(workers) - len(to_close)

        return targets

