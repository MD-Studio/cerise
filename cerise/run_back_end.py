import logging
import signal
import sys
import time
import traceback
import os
import xenon
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import cerise.config

if __name__ == "__main__":
    # Set up Xenon
    xenon.init()
    xenon_ = xenon.Xenon()

    # Load configuration
    config = cerise.config.make_config(xenon_)

    # Set up shut-down handler
    def term_handler(signum, frame):
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
    # Note: needs to be imported after Xenon is inited
    from cerise.back_end.execution_manager import ExecutionManager

    logging.info('Starting up')
    try:
        apidir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'api')
        manager = ExecutionManager(config, apidir, xenon_)
        manager.execute_jobs()
    except:
        logging.critical(traceback.format_exc())

    # Shut down
    logging.info('Shutting down')
    xenon_.close()
