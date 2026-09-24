import re


def worker_type_from_name(name: str, prefix: str) -> str | None:
    """Return the worker type a cluster-generated worker name belongs to.

    Worker names follow the ``{prefix}{wtype}-{i}`` (single-process) or
    ``{prefix}{wtype}-{i}-{p}`` (multi-process) scheme, where ``prefix`` is the
    cluster's ``{self._name}-`` prefix. The type is the remaining text after
    the prefix and any trailing numeric index/process suffixes. Returns ``None``
    for a name that does not start with ``prefix``.
    """
    if not name.startswith(prefix):
        return None
    return re.sub(r"(-\d+){1,2}$", "", name[len(prefix):])


def job_name_from_worker(name: str, processes: int) -> str:
    """Map a registered worker name back to its parent job name.

    Workers with ``processes == 1`` register under the job name itself, so the
    name is returned unchanged. With ``processes > 1`` dask appends a process
    index (``job-0``, ``job-1`` ...); only that suffix is dropped.
    """
    if processes <= 1:
        return name
    return re.sub(r"-\d+$", "", name)


def worker_job_index(name: str) -> int:
    """Return the numeric job index encoded in a worker/job name."""
    match = re.search(r"-(\d+)(?:-\d+)?$", name)
    return int(match.group(1)) if match else 0
