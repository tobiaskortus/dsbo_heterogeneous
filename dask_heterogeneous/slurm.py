from dask_jobqueue.slurm import SLURMJob
from dask_heterogeneous.core import HeterogeneousJobQueueCluster
from dask_heterogeneous.scheduler import HeterogeneousScheduler

class HeterogeneousSlurmCluster(HeterogeneousJobQueueCluster):
    job_cls = SLURMJob
    
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