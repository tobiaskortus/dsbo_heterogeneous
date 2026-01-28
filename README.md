# Dask-Heterogeneous

dask_heteogeneous provides a small library that extends Dask/Dask-Jobqueue to run heterogeneous worker pools in a single dask cluster. The primary goal is simple, predictable per-type scaling plus runtime registration that cleanly integrates with the existing Cluter types (Local, SLURM, ...) provided by dask_jobqueue.

