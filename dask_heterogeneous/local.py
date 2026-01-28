from distributed import Scheduler
from dask_jobqueue.local import LocalJob
from dask_heterogeneous.core import HeterogeneousJobQueueCluster

class HeterogeneousLocalCluster(HeterogeneousJobQueueCluster):
    job_cls = LocalJob

    def __init__(self, spec, n_workers=0, loop=None, security=None, shared_temp_directory=None,
                 silence_logs="error", name=None, asynchronous=False, scheduler_options=None,
                 scheduler_cls=Scheduler, **kwargs):
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