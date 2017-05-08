from .in_memory_job_store import InMemoryJobStore
from .sqlite_job_store import SQLiteJobStore
from .local_files import LocalFiles
from .xenon_remote_files import XenonRemoteFiles
from .xenon_job_runner import XenonJobRunner

import atexit
import signal
import threading
import xenon
import yaml

# The try-except ignores an error from Xenon about double initialisation.
# I'm not doing that as far as I can see, but it seems that PyTest does,
# because without this, I get that error when trying to run the Swagger
# tests.
try:
    xenon.init()
except ValueError:
    pass

config_file_path = 'config.yml'
with open(config_file_path) as config_file:
    config = yaml.safe_load(config_file)

# This is a bit of a belt-and-suspenders approach, but it seems to work.
_xenon_closing_lock = threading.Lock()
with _xenon_closing_lock:
    _xenon = xenon.Xenon()
    _xenon_needs_closing = True

@atexit.register
def close_xenon():
    global _xenon_closing_lock
    global _xenon_needs_closing
    with _xenon_closing_lock:
        if _xenon_needs_closing:
            _xenon.close()
            _xenon_needs_closing = False

def term_handler(signum, frame):
    quit()

signal.signal(signal.SIGINT, term_handler)

# _job_store = InMemoryJobStore()
_job_store = SQLiteJobStore('scxs.db')
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
