"""The process's list of jobs by name, so a job can be submitted by name and listed.

A job is added with an explicit register(job) at the end of the module that defines it, so importing
that module registers it. Modules that define task specs and jobs, and nothing else, are the
definition modules: a web process loads them (load) without any task code.
"""

from collections.abc import Sequence

from taskline.errors import CatalogueError, DuplicateJob
from taskline.job import Job

__all__ = ["DuplicateJob", "register"]

_jobs: dict[str, Job] = {}


def register(job: Job) -> Job:
    """Add a job to the catalogue, and return it, so a module can say JOB = register(Job(...)).

    Registering the same definition again is harmless. A different job with the same name raises
    DuplicateJob.
    """
    known = _jobs.get(job.name)
    if known is not None and known != job:
        raise DuplicateJob(f"a different job named {job.name!r} is already registered")
    _jobs[job.name] = job
    return job


def get(name: str) -> Job | None:
    return _jobs.get(name)


def jobs() -> list[Job]:
    """Every registered job, by name."""
    return [_jobs[name] for name in sorted(_jobs)]


def load(modules: Sequence[str] | str | None = None) -> list[str]:
    """Import the modules that define jobs, so that importing them registers the jobs.

    Without modules, the ones named in TASKLINE_CATALOGUE_MODULES (comma-separated) are loaded.
    A module that cannot be imported raises CatalogueError naming it. Returns the modules loaded;
    loading one already loaded is harmless.
    """
    if modules is None:
        from taskline.config import get_config

        modules = get_config().catalogue_modules
    names = (
        [name.strip() for name in modules.split(",") if name.strip()]
        if isinstance(modules, str)
        else list(modules)
    )
    import importlib

    for name in names:
        try:
            importlib.import_module(name)
        except Exception as error:
            raise CatalogueError(
                f"cannot import module {name!r}: {type(error).__name__}: {error}"
            ) from error
    return names


def _reset() -> None:
    """Forget every job. For tests only."""
    _jobs.clear()
