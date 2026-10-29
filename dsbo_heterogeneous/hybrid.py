from dask_jobqueue.local import LocalJob

from dsbo_heterogeneous.scheduler import HeterogeneousScheduler
from dsbo_heterogeneous.core import HeterogeneousJobQueueCluster


def assert_no_mixed_jobqueue(spec):
    unique_jobqueue_cls = set(value["cls"] for value in spec.values())
    unique_jobqueue_cls.discard(LocalJob)
    unique_jobqueue_names = tuple(cls.__name__ for cls in unique_jobqueue_cls)
    assert len(unique_jobqueue_cls) <= 1, f"The spec contains mixed job queues {unique_jobqueue_names}, which is not allowed."

def assert_all_specs_contain_cls(spec):
    missing = [k for k, v in spec.items() if not (v or {}).get("cls")]
    assert not missing, ("Every worker type in HybridCluster spec must include a 'cls' key "
                         f"with the Job class. Missing 'cls' for: {missing}")


class HybridCluster(HeterogeneousJobQueueCluster):
    def __init__(
        self,
        spec,
        n_workers=None,
        loop=None,
        security=None,
        shared_temp_directory=None,
        silence_logs="error",
        name=None,
        asynchronous=False,
        scheduler_options=None,
        scheduler_cls=HeterogeneousScheduler,
        **kwargs,
    ):
        
        assert_all_specs_contain_cls(spec)
        assert_no_mixed_jobqueue(spec)

        super().__init__(
            n_workers=n_workers,
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
