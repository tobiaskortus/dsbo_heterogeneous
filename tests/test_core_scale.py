import pytest
from unittest.mock import patch

from dask_heterogeneous.local import HeterogeneousLocalCluster


@pytest.fixture
def cluster():
    """A heterogeneous local cluster with cpu/gpu types, without launching jobs."""

    async def _noop_correct_state(self):
        # Prevent `_correct_state` from launching real jobs so we can assert on
        # the spec reconciliation performed by `scale` in isolation.
        return

    spec = {
        "cpu": {"cores": 1, "memory": "1GB", "processes": 1, "resources": {"cpu": 1}},
        "gpu": {"cores": 1, "memory": "1GB", "processes": 1, "resources": {"gpu": 1}},
    }
    with patch.object(HeterogeneousLocalCluster, "_correct_state", _noop_correct_state):
        instance = HeterogeneousLocalCluster(spec, silence_logs="error")
    yield instance
    instance.close()


def worker_names(cluster):
    return set(cluster.worker_spec or {})


def _name(cluster, wtype, index):
    return f"{cluster._name}-{wtype}-{index}"


def test_scale_dict_scales_up_each_type(cluster):
    cluster.scale({"cpu": 2, "gpu": 1})

    assert worker_names(cluster) == {
        _name(cluster, "cpu", 0),
        _name(cluster, "cpu", 1),
        _name(cluster, "gpu", 0),
    }


def test_scale_dict_down_scales_requested_type(cluster):
    cluster.scale({"cpu": 3})
    assert len(worker_names(cluster)) == 3

    cluster.scale({"cpu": 1})
    assert worker_names(cluster) == {_name(cluster, "cpu", 0)}


def test_scale_type_to_zero_removes_type(cluster):
    cluster.scale({"cpu": 2, "gpu": 1})

    cluster.scale({"cpu": 0})
    assert worker_names(cluster) == {_name(cluster, "gpu", 0)}


def test_scale_down_preserves_untouched_types(cluster):
    cluster.scale({"cpu": 3, "gpu": 2})

    cluster.scale({"cpu": 1})

    assert len(worker_names(cluster)) == 3
    assert {_name(cluster, "gpu", 0), _name(cluster, "gpu", 1)} <= worker_names(cluster)


def test_scale_dict_unknown_type_raises(cluster):
    with pytest.raises(KeyError, match="Unknown worker type"):
        cluster.scale({"tpu": 1})
