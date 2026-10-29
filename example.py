import dask
from dsbo_heterogeneous.local import HeterogeneousLocalCluster
from dask.distributed import Client

from dsbo_heterogeneous.scheduler import HeterogeneousScheduler

spec = {
    "small": {"cores": 1, "memory": "1GB", "processes": 1, "resources": {"small": 1}},
    "big": {"cores": 4, "memory": "1GB", "processes": 1, "resources": {"big": 1}},
    "xlarge": {"cores": 2, "memory": "1GB", "processes": 1, "resources": {"xlarge": 1}}
}

import time
if __name__ == "__main__":
    cluster = HeterogeneousLocalCluster(spec=spec, n_workers=0, scheduler_cls=HeterogeneousScheduler)
    #cluster.add_worker("xlarge", {"cores": 2, "memory": "1GB", "processes": 1, "resources": {"xlarge": 1}})
    #cluster.adapt(minimum_jobs=1, maximum_jobs=10)
    cluster.adapt({"xlarge": (0, 20), "small": (0, 2), "big": (0, 1)})

    print("dashboard:", cluster.dashboard_link)

    with Client(cluster, timeout=5) as client:
        def y(x):
            from dask.distributed import get_worker
            time.sleep(2.5)
            worker = get_worker()
            return (x, worker.name)

        print("First submit cycle")
        res = client.submit(y, 42, resources={"xlarge": 1}).result()

        print("Second submit cycle")
        resres = client.gather(client.map(y, range(100), resources={"xlarge": 1}))

        print("Last submit cycle")
        #resres = client.gather(client.map(y, range(50), resources={"xlarge": 1}))
        res = client.gather([client.submit(y, i, resources={"xlarge": 1}) for i in range(50)])
        time.sleep(1000)
        

