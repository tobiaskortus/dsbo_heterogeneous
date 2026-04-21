import pytest

from dask_jobqueue.local import LocalJob
from dask_jobqueue.slurm import SLURMJob
from dask_jobqueue.sge import SGEJob

from dask_heterogeneous.hybrid import assert_no_mixed_jobqueue, assert_all_specs_contain_cls, HybridCluster

def test_mixed_jobqueue():
    with pytest.raises(AssertionError, match="The spec contains mixed job queues"):
        spec = {
            "a": {"cls": LocalJob},
            "b": {"cls": SLURMJob},
            "c": {"cls": SLURMJob},
            "d": {"cls": SGEJob},
        }

        assert_no_mixed_jobqueue(spec)


def test_mixed_jobqueue_only_local_jobs():
    spec = {
        "a": {"cls": LocalJob},
        "b": {"cls": LocalJob},
        "c": {"cls": LocalJob},
    }

    assert_no_mixed_jobqueue(spec)


def test_spec_without_cls_raises():
    with pytest.raises(AssertionError, match="Every worker type in HybridCluster spec must include a 'cls' key"):
        spec = {
            "a": {"cores": 1, "memory": "1GB"},
            "b": {"cls": SLURMJob, "cores": 2, "memory": "2GB"},
        }

        assert_all_specs_contain_cls(spec)


def test_hybrid_cluster_job_header():
    spec = {
        "a": {"cls": LocalJob, "cores": 1, "memory": "1GB"},
        "b": {"cls": SLURMJob, "cores": 2, "memory": "2GB"},
    }

    cluster = HybridCluster(spec)
    assert cluster.job_header["a"] == ""
    assert "#SBATCH" in cluster.job_header["b"]