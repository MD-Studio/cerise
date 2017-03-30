from .in_memory_job_store import InMemoryJobStore
from .local_files import LocalFiles
from .xenon_remote_files import XenonRemoteFiles
from .xenon_job_runner import XenonJobRunner

import xenon
import yaml

# The try-except ignores an error from Xenon about double initialisation.
# I'm not doing that as far as I can see, but it seems that PyTest does,
# because without this, I get that error when trying to run the Swagger
# tests. It still sometimes throws on shutdown there.
try:
    xenon.init()
except ValueError:
    pass

config_file_path = 'config.yml'
with open(config_file_path) as config_file:
    config = yaml.load(config_file)

_xenon = xenon.Xenon()
_job_store = InMemoryJobStore()
_local_files = LocalFiles(_job_store, config['local'])
_remote_files = XenonRemoteFiles(_job_store, _xenon, config['compute-resource'])
_job_runner = XenonJobRunner(_job_store, _xenon, config['compute-resource'])

def job_store():
    return _job_store

def local_files():
    return _local_files

def remote_files():
    return _remote_files

def job_runner():
    return _job_runner
