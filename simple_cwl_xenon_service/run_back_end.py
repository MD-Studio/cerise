import atexit
import logging
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
    logging.info('Back-end shut down requested')
    manager.shutdown()
    raise KeyboardInterrupt

signal.signal(signal.SIGTERM, term_handler)
signal.signal(signal.SIGINT, term_handler)

if 'pidfile' in config:
    with open(config['pidfile'], 'w') as f:
        f.write(str(os.getpid()))

# Set up logging
if 'logging' in config:
    logfile = config['logging'].get('file', '/var/log/scxs/scxs_backend.log')
    loglevel_str = config['logging'].get('level', 'INFO')
    loglevel = getattr(logging, loglevel_str.upper(), None)
    logging.basicConfig(filename=logfile, level=loglevel,
            format='[%(asctime)s.%(msecs)03d] [%(levelname)s] %(message)s [%(name)s]',
            datefmt='%Y-%m-%d %H:%M:%S')

# Run
logging.info('Starting up')
manager = ExecutionManager(config, _xenon)
manager.execute_jobs()

# Shut down
logging.info('Shutting down')
_xenon.close()

