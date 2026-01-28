from dask_heterogeneous.local import HeterogeneousLocalCluster
from dask.distributed import Client

spec = {
    "small": {"cores": 1, "memory": "1GB", "processes": 1, "resources": {"small": 1}},
    "big": {"cores": 4, "memory": "1GB", "processes": 2, "resources": {"big": 1}},
}

import time
if __name__ == "__main__":
    cluster = HeterogeneousLocalCluster(spec=spec, n_workers=0)
    cluster.add_worker("xlarge", {"cores": 2, "memory": "1GB", "processes": 1, "resources": {"xlarge": 1}})
    cluster.scale({"small": 1, "big": 1, "xlarge": 5})

    print("dashboard:", getattr(cluster, "dashboard_link", None))

    with Client(cluster, timeout=5) as client:
        def y(x):
            from dask.distributed import get_worker
            worker = get_worker()
            return (x, worker.name)

        # Submit using Dask resources (must match worker `resources` mapping)
        res = client.submit(y, 42, resources={"xlarge": 1}).result()
        print(res)
        time.sleep(1000)
        

