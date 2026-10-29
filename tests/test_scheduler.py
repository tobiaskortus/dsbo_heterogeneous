import dask
import pytest
import threading
from distributed import Client
from unittest.mock import patch
from distributed.deploy.spec import SpecCluster

from dsbo_heterogeneous.scheduler import HeterogeneousScheduler


def function(value):
    return value


class ObservableScheduler(HeterogeneousScheduler):
    """Signal tests after the normal scheduler graph-ingest path completes."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.graph_updated = threading.Event()

    async def update_graph(self, *args, **kwargs):
        try:
            return await super().update_graph(*args, **kwargs)
        finally:
            self.graph_updated.set()


@pytest.fixture
def cluster():
    scheduler_spec = {
        "cls": ObservableScheduler,
        "options": {
            "protocol": "inproc://",
            "dashboard": False,
            "dashboard_address": None,
        },
    }
    with patch.object(ObservableScheduler, "start_http_server"):
        instance = SpecCluster(
            scheduler=scheduler_spec,
            workers={},
            asynchronous=False,
            silence_logs="error",
        )
    instance.scheduler._spec_workers = {"cpu": {}, "gpu": {}}
    yield instance
    instance.close()


@pytest.fixture
def client(cluster):
    with Client(cluster) as instance:
        yield instance


def wait_for_graph(cluster):
    assert cluster.scheduler.graph_updated.wait(timeout=1)


def test_submit_with_resources_routes_task_to_worker_type(client, cluster):
    future = client.submit(function, 1, resources={"gpu": 1})
    wait_for_graph(cluster)

    task = cluster.scheduler.tasks[future.key]
    assert task.resource_restrictions == {"gpu": 1}
    assert cluster.scheduler._task_worker_type(task) == "gpu"


def test_dask_annotate_resources_routes_task_to_worker_type(client, cluster):
    with dask.annotate(resources={"gpu": 1}):
        future = client.submit(function, 1, key="annotated-submit")

    wait_for_graph(cluster)

    task = cluster.scheduler.tasks[future.key]
    assert task.resource_restrictions == {"gpu": 1}
    assert cluster.scheduler._task_worker_type(task) == "gpu"


def submit_with_resources(submission, annotate, resources):
    if annotate:
        with dask.annotate(resources=resources):
            return submission(function, 1)
    return submission(function, 1, resources=resources)


def test_submit_without_resources_raises_informative_error(client, cluster):
    future = client.submit(function, 1)

    with pytest.raises(ValueError) as excinfo:
        future.result(timeout=1)

    message = str(excinfo.value)
    assert "resources" in message
    assert "dask.annotate" in message


@pytest.mark.parametrize(
    "annotate,resources",
    [
        (False, {"gpu": 1, "cpu": 1}),
        (True, {"gpu": 1, "cpu": 1}),
    ],
)
def test_with_multiple_resources_raises_ambiguous_routing_error(client, cluster, annotate, resources):
    future = submit_with_resources(client.submit, annotate, resources)

    with pytest.raises(ValueError, match="multiple resources"):
        future.result(timeout=1)


@pytest.mark.parametrize(
    "annotate,resources",
    [
        (False, {"tpu": 1}),
        (True, {"tpu": 1}),
    ],
)
def test_with_unknown_worker_type_raises_error(client, cluster, annotate, resources):
    future = submit_with_resources(client.submit, annotate, resources)

    with pytest.raises(ValueError) as excinfo:
        future.result(timeout=1)

    message = str(excinfo.value)
    assert "'tpu'" in message
    assert "cpu" in message
    assert "gpu" in message


def test_submitted_task_resources_drive_queue_grouping(client, cluster):
    future = client.submit(function, 1, resources={"gpu": 1},)
    wait_for_graph(cluster)
    task = cluster.scheduler.tasks[future.key]

    cluster.scheduler.unrunnable.clear()
    cluster.scheduler.queued = [task]

    assert cluster.scheduler.queue_groups == {"gpu": [task]}


def test_unrunnable_gpu_task_produces_gpu_adaptive_target(client, cluster):
    future = client.submit(function, 1, resources={"gpu": 1})
    wait_for_graph(cluster)
    task = cluster.scheduler.tasks[future.key]

    targets = cluster.scheduler.adaptive_target(
        worker_types=["cpu", "gpu"],
        target_duration=5,
    )

    assert task.state == "no-worker"
    assert targets == {"cpu": 0, "gpu": 1}
