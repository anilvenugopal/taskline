"""The process's list of jobs by name, so a job can be submitted by name.

A job is added with an explicit register(job) at the end of the module that defines it, so importing
that module registers it. Modules that define task specs and jobs, and nothing else, can be loaded by
a web process without any task code.
"""

from taskline.errors import DuplicateJob
from taskline.job import Job

__all__ = ["register"]

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


def _reset() -> None:
    """Forget every job. For tests only."""
    _jobs.clear()
