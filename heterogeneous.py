from dask_jobqueue.local import LocalJob, LocalCluster
from dask_jobqueue.core import cluster_parameters, Job, Status, Security, JobQueueCluster

import dask
import math
import re
import copy
import tempfile
import os
import warnings

from distributed import Client
from distributed.deploy.spec import SpecCluster
from distributed.scheduler import Scheduler


class HeterogeneousJobQueueCluster(JobQueueCluster):
    __doc__ = """ Deploy Dask on a Job queuing system

    This is a superclass, and is rarely used directly.  It is more common to
    use an object like SGECluster, SLURMCluster, PBSCluster, LSFCluster, or
    others.

    However, it can be used directly if you have a custom ``Job`` type.
    This class relies heavily on being passed a ``Job`` type that is able to
    launch one Job on a job queueing system.

    Parameters
    ----------
    Job : Job
        A class that can be awaited to ask for a single Job
    {cluster_parameters}
    """.format(
        cluster_parameters=cluster_parameters
    )

    def __init__(
        self,
        n_workers=0,
        job_cls: Job = None,
        spec: dict = None,
        loop=None,
        security=None,
        shared_temp_directory=None,
        silence_logs="error",
        name=None,
        asynchronous=False,
        dashboard_address=None,
        host=None,
        scheduler_options=None,
        scheduler_cls=Scheduler, 
        interface=None,
        protocol=None,
        config_name=None,
        **job_kwargs
    ):
        self.status = Status.created

        default_job_cls = getattr(type(self), "job_cls", None)
        self.job_cls = default_job_cls
        if job_cls is not None:
            self.job_cls = job_cls

        if self.job_cls is None:
            raise ValueError(
                "You need to specify a Job type. Two cases:\n"
                "- you are inheriting from JobQueueCluster (most likely): you need to add a 'job_cls' class variable "
                "in your JobQueueCluster-derived class {}\n"
                "- you are using JobQueueCluster directly (less likely, only useful for tests): "
                "please explicitly pass a Job type through the 'job_cls' parameter.".format(
                    type(self)
                )
            )

        if dashboard_address is not None:
            raise ValueError(
                "Please pass 'dashboard_address' through 'scheduler_options': use\n"
                'cluster = {0}(..., scheduler_options={{"dashboard_address": ":12345"}}) rather than\n'
                'cluster = {0}(..., dashboard_address="12435")'.format(
                    self.__class__.__name__
                )
            )

        if host is not None:
            raise ValueError(
                "Please pass 'host' through 'scheduler_options': use\n"
                'cluster = {0}(..., scheduler_options={{"host": "your-host"}}) rather than\n'
                'cluster = {0}(..., host="your-host")'.format(self.__class__.__name__)
            )

        default_config_name = self.job_cls.default_config_name()
        if config_name is None:
            config_name = default_config_name

        if interface is None:
            interface = dask.config.get("jobqueue.%s.interface" % config_name)
        if scheduler_options is None:
            scheduler_options = dask.config.get(
                "jobqueue.%s.scheduler-options" % config_name, {}
            )

        if protocol is None and security is not None:
            protocol = "tls://"

        if security is True:
            try:
                security = Security.temporary()
            except ImportError:
                raise ImportError(
                    "In order to use TLS without pregenerated certificates `cryptography` is required,"
                    "please install it using either pip or conda"
                )

        default_scheduler_options = {
            "protocol": protocol,
            "dashboard_address": ":8787",
            "security": security,
        }

        scheduler_options = dict(default_scheduler_options, **scheduler_options)

        if "host" not in scheduler_options and "interface" not in scheduler_options:
            scheduler_options["interface"] = interface

        scheduler = {
            "cls": scheduler_cls,
            "options": scheduler_options,
        }

        if shared_temp_directory is None:
            shared_temp_directory = dask.config.get(
                "jobqueue.%s.shared-temp-directory" % config_name
            )
        self.shared_temp_directory = shared_temp_directory

        job_kwargs["config_name"] = config_name
        job_kwargs["interface"] = interface
        job_kwargs["protocol"] = protocol
        job_kwargs["security"] = self._get_worker_security(security)

        self._job_kwargs = job_kwargs

        if spec is not None:
            workers = {}
            for type_name, job_opts in spec.items():
                opts = dict(self._job_kwargs or {})
                if job_opts:
                    opts.update(job_opts)
                # Translate 'resources' dict into worker CLI args so that the
                # underlying Job/worker process advertises resources to Dask.
                # dask-jobqueue Job classes don't accept a 'resources' kwarg,
                # but they accept 'worker_extra_args' which are appended to the
                # worker command. Convert resources -> ['--resources', 'k=v', ...].
                if "resources" in opts:
                    resources = opts.pop("resources") or {}
                    extra = list(opts.get("worker_extra_args") or [])
                    for k, v in (resources.items() if isinstance(resources, dict) else []):
                        extra.extend(["--resources", f"{k}={v}"])
                    if extra:
                        opts["worker_extra_args"] = extra
                # ensure a config_name exists for Job classes that require it
                if "config_name" not in opts:
                    try:
                        opts["config_name"] = self.job_cls.default_config_name()
                    except Exception:
                        opts["config_name"] = getattr(self.job_cls, "config_name", None)
                workers[type_name] = {"cls": self.job_cls, "options": opts}

            # Initialize SpecCluster directly with heterogeneous workers to
            # avoid calling JobQueueCluster.__init__ which expects single-job
            # semantics and would treat our `workers`/`scheduler` kwargs as
            # job kwargs (causing duplicate 'scheduler' entries).
            # Choose a stable cluster name (avoid collisions with spec keys).
            cluster_name = name if name is not None else f"jobqueue-{os.getpid()}"

            # Initialize SpecCluster with an empty `workers` mapping so that
            # the per-type templates in `workers` are kept only in
            # `_spec_workers` and do not create placeholder worker names.
            SpecCluster.__init__(
                self,
                scheduler=scheduler,
                workers={},
                loop=loop,
                security=security,
                silence_logs=silence_logs,
                asynchronous=asynchronous,
                name=cluster_name,
            )
            # Ensure the internal name matches our chosen name (avoid any
            # accidental overrides inside SpecCluster or elsewhere).
            try:
                self._name = cluster_name
            except Exception:
                pass

            # Keep common job kwargs for compatibility with other methods
            self._job_kwargs = job_kwargs

            # Keep a template of per-spec worker templates for dict-based scaling
            # {type_name: {'cls': cls, 'options': opts, 'group': ...}}
            self._spec_workers = {}
            for k, v in (workers or {}).items():
                entry = {"cls": v["cls"], "options": dict(v.get("options", {}))}
                if "group" in v:
                    entry["group"] = list(v["group"])
                self._spec_workers[k] = entry

            # Per-type counters for deterministic instance numbering
            self._type_counters = {k: 0 for k in self._spec_workers}

            if n_workers:
                # For heterogeneous scaling, allow dict-based scale calls
                if isinstance(n_workers, dict):
                    self.scale(n_workers)
                else:
                    self.scale(n_workers)
            return
        
        raise Exception("spec parameter is required for HeterogeneousJobQueueCluster")

    def scale(self, n: int | dict | None = None, jobs=0, memory=None, cores=None):
        if isinstance(n, dict) and not hasattr(self, "_spec_workers"):
            raise ValueError(
                "This cluster was not initialized with a heterogeneous `spec`, so "
                "cannot be scaled using a dict. Please use an integer value for `n`."
            )
        
        if hasattr(self, "_spec_workers") and self._spec_workers is not None:
            if isinstance(n, dict):
                desired = {}

                current = dict(self.worker_spec or {})

                for wtype, requested in n.items():
                    if wtype not in self._spec_workers:
                        raise KeyError(f"Unknown worker type: {wtype}")
                    template = self._spec_workers[wtype]

                    for i in range(int(requested)):
                        name = f"{self._name}-{wtype}-{i}"
                        if name in current:
                            desired[name] = current[name]
                        else:
                            entry = {"cls": template["cls"], "options": dict(template["options"])}
                            if "group" in template:
                                entry["group"] = list(template["group"])
                            desired[name] = entry

                for name, spec_entry in current.items():
                    if name in (self._spec_workers or {}):
                        continue

                    if name not in desired:
                        desired[name] = spec_entry

                self.worker_spec = desired
                self.loop.add_callback(self._correct_state)
                return

            if n == 0:
                self.worker_spec = {}
                self.loop.add_callback(self._correct_state)
                return

        if n is not None:
            jobs = int(math.ceil(n / self._dummy_job.worker_processes))

        return super().scale(jobs, memory=memory, cores=cores)

    def adapt(
        self, *args, minimum_jobs: int = None, maximum_jobs: int = None, **kwargs
    ):
        raise Exception("adapt method is not implemented for HeterogeneousJobQueueCluster")
        if minimum_jobs is not None:
            kwargs["minimum"] = minimum_jobs * self._dummy_job.worker_processes
        if maximum_jobs is not None:
            kwargs["maximum"] = maximum_jobs * self._dummy_job.worker_processes
        return super().adapt(*args, **kwargs)

    def add_worker(self, type_name: str, options: dict | None = None):
        """Register a new worker-type spec after initialization.

        Parameters
        ----------
        type_name : str
            Name for the worker type.
        options : dict, optional
            Worker options for this type; merged with the cluster's common job kwargs.
        """
        if not hasattr(self, "_spec_workers") or self._spec_workers is None:
            self._spec_workers = {}

        opts = dict(self._job_kwargs or {})
        if options:
            opts.update(options)

        if "config_name" not in opts:
            try:
                opts["config_name"] = self.job_cls.default_config_name()
            except Exception:
                opts["config_name"] = getattr(self.job_cls, "config_name", None)

        if "resources" in opts:
            resources = opts.pop("resources") or {}
            extra = list(opts.get("worker_extra_args") or [])
            for k, v in (resources.items() if isinstance(resources, dict) else []):
                extra.extend(["--resources", f"{k}={v}"])
            if extra:
                opts["worker_extra_args"] = extra

        self._spec_workers[type_name] = {"cls": self.job_cls, "options": opts}

        if not hasattr(self, "_type_counters"):
            self._type_counters = {}
        self._type_counters.setdefault(type_name, 0)
        self.loop.add_callback(self._correct_state)



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
        

