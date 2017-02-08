from .in_memory_job_store import InMemoryJobStore
from .xenon_job_runner import XenonJobRunner

_job_store = InMemoryJobStore()
_job_runner = XenonJobRunner(_job_store)

def job_store():
    return _job_store

def job_runner():
    return _job_runner
