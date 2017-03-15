from .in_memory_job_store import InMemoryJobStore
from .xenon_job_runner import XenonJobRunner

import xenon

# The try-except ignores an error from Xenon about double initialisation.
# I'm not doing that as far as I can see, but it seems that PyTest does,
# because without this, I get that error when trying to run the Swagger
# tests. It still sometimes throws on shutdown there.
try:
    xenon.init()
except ValueError:
    pass

_job_store = InMemoryJobStore()
_job_runner = XenonJobRunner(_job_store)

def job_store():
    return _job_store

def job_runner():
    return _job_runner
