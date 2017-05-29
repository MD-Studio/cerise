import atexit
import signal
import threading
import xenon

# The try-except ignores an error from Xenon about double initialisation.
# I'm not doing that as far as I can see, but it seems that PyTest does,
# because without this, I get that error when trying to run the Swagger
# tests.
try:
    xenon.init()
except ValueError:
    pass

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

# The central job manager object
from simple_cwl_xenon_service.config import config
from .job_manager import JobManager

job_manager = JobManager(config, _xenon)
