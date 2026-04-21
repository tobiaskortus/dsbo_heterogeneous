# Dask-Heterogeneous

dask_heteogeneous provides a small library that extends Dask/Dask-Jobqueue to run heterogeneous worker pools in a single dask cluster. The primary goal is simple, predictable per-type scaling plus runtime registration that cleanly integrates with the existing Cluter types (Local, SLURM, ...) provided by dask_jobqueue.


> [!WARNING]
> dsbo is a prototype/early stage implementation for heterogeneous cluster creation within dask-jobqueue! If you notice any issues or have any suggestions for improvements, please write up a GitHub issue detailing the problem or proposal. We also recommend “watching” the GitHub repository to keep track of new releases, and upgrading promptly when they occur.


## Installation

```sh
pip install git+git@github.com:tobiaskortus/dask_heterogeneous.git
```


## Usage 

```py
from dask_heterogeneous.slurm import HeterogeneousSlurmCluster
from dask.distributed import Client


spec = {
    "cpu": {"cores": 1, "memory": "1GB", "processes": 1, "resources": {"small": 1}},
    "gpu": {"cores": 4, "memory": "1GB", "processes": 1, "resources": {"big": 1}},
}

cluster = HeterogeneousSlurmCluster(spec)
cluster.adapt({"cpu": (0, 20), "gpu": (0, 5)})
client = Client(cluster)

```

```py
from dask_heterogeneous.mixed import MixedHeterogeneousCluster
from dask_jobqueue.local import LocalJob
from dask_jobqueue.slurm import SLURMJob
from dask.distributed import Client


spec = {
    "cpu": {"cls": LocalJob, 
            "cores": 1, "memory": "1GB", "processes": 1, 
            "resources": {"small": 1}},
    "gpu": {"cls": SLURMJob, 
            "cores": 4, "memory": "1GB", "processes": 1, 
            "resources": {"big": 1}},
}

cluster = MixedHeterogeneousCluster(spec)
cluster.adapt({"cpu": (0, 20), "gpu": (0, 5)})
client = Client(cluster)

```
