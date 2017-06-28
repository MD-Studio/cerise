import atexit
import signal
import sys
import threading
import os
import xenon
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from simple_cwl_xenon_service.config import config
from back_end.execution_manager import ExecutionManager

# Set up Xenon
xenon.init()
_xenon = xenon.Xenon()

# Set up shut-down handler
def term_handler(signum, frame):
    manager.shutdown()
    raise KeyboardInterrupt

signal.signal(signal.SIGTERM, term_handler)
signal.signal(signal.SIGINT, term_handler)

if 'pidfile' in config:
    with open(config['pidfile'], 'w') as f:
        f.write(str(os.getpid()))

# Run
manager = ExecutionManager(config, _xenon)
manager.execute_jobs()

# Shut down
_xenon.close()

