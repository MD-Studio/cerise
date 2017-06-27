import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import atexit

from back_end import x

from simple_cwl_xenon_service.config import config
from back_end.execution_manager import ExecutionManager

manager = ExecutionManager(config, x)

@atexit.register
def shutdown():
    manager.shutdown()

manager.execute_jobs()
