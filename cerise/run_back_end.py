import atexit
import logging
import signal
import sys
import time
import traceback
import os
import xenon
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from cerise.config import config
from cerise.config import api_config

# Set up Xenon
xenon.init()
_xenon = xenon.Xenon()

# Set up shut-down handler
def term_handler(signum, frame):
    logging.info('Back-end shut down requested')
    manager.shutdown()
    time.sleep(1)
    raise KeyboardInterrupt

signal.signal(signal.SIGTERM, term_handler)
signal.signal(signal.SIGINT, term_handler)

if 'pidfile' in config:
    with open(config['pidfile'], 'w') as f:
        f.write(str(os.getpid()))

# Set up logging
if 'logging' in config:
    logfile = config['logging'].get('file', '/var/log/cerise/cerise_backend.log')
    loglevel_str = config['logging'].get('level', 'INFO')
    loglevel = getattr(logging, loglevel_str.upper(), None)
    logging.basicConfig(filename=logfile, level=loglevel,
            format='[%(asctime)s.%(msecs)03d] [%(levelname)s] %(message)s [%(name)s]',
            datefmt='%Y-%m-%d %H:%M:%S')

# Run
# Note: needs to be imported after Xenon is inited
from back_end.execution_manager import ExecutionManager

logging.info('Starting up')
try:
    apidir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'api')
    manager = ExecutionManager(config, api_config, apidir, _xenon)
    manager.execute_jobs()
except:
    logging.critical(traceback.format_exc())

# Shut down
logging.info('Shutting down')
_xenon.close()

