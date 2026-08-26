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
