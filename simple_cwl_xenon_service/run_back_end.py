import atexit
import sys
import os
import xenon
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from simple_cwl_xenon_service.config import config
from back_end.execution_manager import ExecutionManager

# Set up Xenon
xenon.init()
_xenon = xenon.Xenon()

# Run
manager = ExecutionManager(config, _xenon)

@atexit.register
def shutdown():
    manager.shutdown()

manager.execute_jobs()

# Shut down
_xenon.close()

