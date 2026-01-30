from dask_heterogeneous.slurm import HeterogeneousSlurmCluster
from dask.distributed import Client

spec = {
    "small": {"cores": 1, "memory": "1GB", "processes": 1, "resources": {"small": 1}},
    "big": {"cores": 4, "memory": "1GB", "processes": 2, "resources": {"big": 1}},
}


def get_ressources_key(spec_config: dict, name: str) -> dict:
    return spec_config[name]["resources"]

import time
if __name__ == "__main__":
    cluster = HeterogeneousSlurmCluster(spec=spec, n_workers=0)
    cluster.add_worker("xlarge", {"cores": 2, "memory": "1GB", "processes": 1, "resources": {"xlarge": 1}})
    cluster.scale({"small": 1, "big": 1, "xlarge": 5})

    print("dashboard:", getattr(cluster, "dashboard_link", None))

    with Client(cluster, timeout=5) as client:
        def y(x):
            from dask.distributed import get_worker
            worker = get_worker()
            return (x, worker.name)

        # Submit using Dask resources (must match worker `resources` mapping)
        res = client.submit(y, 42, resources=get_ressources_key(spec, "big")).result()
        print(res)
        time.sleep(1000)
        



