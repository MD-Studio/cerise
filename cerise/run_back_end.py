import logging
import signal
import sys
import time
import traceback
from types import FrameType
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import cerise.config
from cerise.back_end.execution_manager import ExecutionManager


if __name__ == "__main__":
    # Load configuration
    config = cerise.config.make_config()

    # Set up shut-down handler
    def term_handler(signum: int, frame: FrameType) -> None:
        logging.info('Back-end shut down requested')
        manager.shutdown()

    signal.signal(signal.SIGTERM, term_handler)
    signal.signal(signal.SIGINT, term_handler)

    pid_file = config.get_pid_file()
    if pid_file:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))

    # Set up logging
    if config.has_logging():
        logfile = config.get_log_file()
        loglevel = config.get_log_level()
        logging.basicConfig(filename=logfile, level=loglevel,
                format='[%(asctime)s.%(msecs)03d] [%(levelname)s] %(message)s [%(name)s]',
                datefmt='%Y-%m-%d %H:%M:%S')

    # Run
    logging.info('Starting up')
    try:
        apidir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'api')
        manager = ExecutionManager(config, apidir)
        manager.execute_jobs()
    except:
        logging.critical(traceback.format_exc())

    # Shut down
    logging.info('Shutting down')
