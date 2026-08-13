from collections import defaultdict
import os
import math
import re
from typing import overload
from distributed.deploy.spec import SpecCluster
from dask_jobqueue.core import cluster_parameters, Job, Status, Security, JobQueueCluster
from dask_heterogeneous.scheduler import HeterogeneousScheduler

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
        scheduler_cls=HeterogeneousScheduler,
        interface=None,
        protocol=None,
        **job_kwargs
    ):
        self.status = Status.created
        self.adapt_config = defaultdict(int)

        default_job_cls = getattr(type(self), "job_cls", None)
        self.job_cls = default_job_cls
        if job_cls is not None:
            self.job_cls = job_cls

        if not self.job_cls and not all(v and v.get("cls") for v in (spec or {}).values()):
            raise ValueError(
                "Cannot determine the Job class/es. Include a 'cls' key when using HybridCluster."
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

        if scheduler_options is None:
            scheduler_options = {}

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

        self.shared_temp_directory = shared_temp_directory

        job_kwargs["interface"] = interface
        job_kwargs["protocol"] = protocol
        job_kwargs["security"] = self._get_worker_security(security)

        self._job_kwargs = job_kwargs

        if spec is not None:
            cluster_name = name.replace("-", "_") if name is not None else f"jobqueue_{os.getpid()}"

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

            try:
                self._name = cluster_name
            except Exception:
                pass

            self._spec_workers = {}
            for type_name, job_opts in spec.items():
                if job_opts is {}:
                    continue

                spec_job_cls = job_opts.get("cls", None) or self.job_cls
                if spec_job_cls is None:
                    raise ValueError(
                        f"Worker type '{type_name}' has no 'spec' key and no cluster-level "
                        "job_cls is set. Add a 'spec' key to this entry or pass job_cls."
                    )
                job_opts.pop("cls", None)
                opts = dict(self._job_kwargs or {})
                opts.update(job_opts)
                self._resources_to_worker_extra_args(opts)
                opts["config_name"] = spec_job_cls.default_config_name()
                self._spec_workers[type_name] = {"cls": spec_job_cls, "options": opts}

            self._type_counters = {k: 0 for k in self._spec_workers}

            if n_workers:
                self.scale(n_workers)
            return
        
        raise Exception("spec parameter is required for HeterogeneousJobQueueCluster")

    
    @overload
    def scale(self, n: int | None = None): ...

    @overload
    def scale(self, n_per_worker: dict | None = None): ...


    def scale(self, n: int | dict | None = None):
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
            if hasattr(self, "_spec_workers") and self._spec_workers:
                desired_procs = int(n)

                scale_dict = {}
                for wtype, template in self._spec_workers.items():
                    opts = template.get("options", {}) or {}
                    worker_processes = opts.get("processes", 1)
                    jobs_for_type = int(math.ceil(desired_procs / worker_processes))
                    scale_dict[wtype] = jobs_for_type

                return self.scale(scale_dict)

        raise Exception("Invalid scale config")
        
    @overload
    def adapt(self, min_max_per_worker: dict, **kwargs): ...

    @overload
    def adapt(self, minimum_jobs: int = None, maximum_jobs: int = None, **kwargs): ...
    

    def adapt(
        self, *args, **kwargs
    ):
        scale = lambda v, p: None if v is None else int(v) * p

        per_worker = (
            args[0]
            if args and isinstance(args[0], dict)
            else kwargs.pop("min_max_per_worker", None)
        )

        if per_worker is None:
            if args and len(args) >= 2:
                min_j, max_j = args[0], args[1]
            else:
                min_j = kwargs.pop("minimum_jobs", None)
                max_j = kwargs.pop("maximum_jobs", None)
                
        if per_worker is not None:
            for wtype, (min_v, max_v) in per_worker.items():
                if wtype not in (self._spec_workers or {}):
                    raise KeyError(f"Unknown worker type: {wtype}")
                
                procs = self._spec_workers[wtype].get("options", {}).get("processes", 1)
                self.adapt_config[wtype] = (scale(min_v, procs), scale(max_v, procs))
        else:
            if min_j is None and max_j is None:
                #TODO: Check if None is a valid options for dask_jobqueue
                raise TypeError("adapt() requires min/max or per-worker mapping")
            
            for wtype in (self._spec_workers or {}):
                procs = self._spec_workers[wtype].get("options", {}).get("processes", 1)
                self.adapt_config[wtype] = (scale(min_j, procs), scale(max_j, procs))

        kwargs["adapt_config"] = dict(self.adapt_config)
        from dask_heterogeneous.adaptive import HeterogeneousAdaptive
        return super().adapt(Adaptive=HeterogeneousAdaptive, **kwargs)

    def add_worker(self, type_name: str, options: dict | None = None):
        """Register a new worker-type spec after initialization.

        Parameters
        ----------
        type_name : str
            Name for the worker type.
        options : dict, optional
            Worker options for this type; merged with the cluster's common job kwargs.
        """
        raise NotImplementedError(
            "add_worker is currently not supported. Please specify all worker "
            "types at cluster initialization through the `spec` parameter."
        )


    def _resources_to_worker_extra_args(self, opts: dict) -> dict:
        """Convert a `resources` mapping in `opts` into `worker_extra_args`.

        Removes the `resources` key from `opts` (if present) and appends
        `--resources key=value` pairs to the `worker_extra_args` list so that
        the Job constructor does not receive an unexpected `resources` kwarg.
        Returns the updated `opts` dict.
        """
        if not isinstance(opts, dict):
            return opts

        if "resources" in opts:
            resources = opts.pop("resources") or {}
            extra = list(opts.get("worker_extra_args") or [])
            for k, v in (resources.items() if isinstance(resources, dict) else []):
                extra.extend(["--resources", f"{k}={v}"])
            if extra:
                opts["worker_extra_args"] = extra

        return opts


    def _make_dummy_job(self, type_name: str):
        """Instantiate a dummy Job for a specific worker type."""
        try:
            address = self.scheduler.address
        except AttributeError:
            address = "tcp://<insert-scheduler-address-here>:8786"
        entry = self._spec_workers[type_name]
        opts = dict(entry.get("options", {}) or {})
        try:
            return entry["cls"](
                address or "tcp://<insert-scheduler-address-here>:8786",
                name="dummy-name",
                **opts,
            )
        except TypeError as exc:
            match = re.search("(unexpected keyword argument.+)", str(exc))
            if not match:
                raise
            message_orig = match.group(1)
            raise ValueError(
                'Got {}. Very likely this unexpected parameter was passed in "job_kwargs" in the {} constructor:\n'
                "job_kwargs={}".format(message_orig, self.__class__.__name__, self._job_kwargs)
            ) from exc

    @property
    def _dummy_job(self):
        """Create a representative Job using the first worker-type's config."""
        first_type = next(iter(self._spec_workers))
        return self._make_dummy_job(first_type)

    @property
    def job_header(self) -> dict[str, str]:
        return {type_name: self._make_dummy_job(type_name).job_header for type_name in self._spec_workers}

    def job_script(self) -> dict[str, str]:
        return {type_name: self._make_dummy_job(type_name).job_script() for type_name in self._spec_workers}