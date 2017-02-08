from .in_memory_job_store import InMemoryJobStore

_job_store = InMemoryJobStore()

def job_store():
    return _job_store
