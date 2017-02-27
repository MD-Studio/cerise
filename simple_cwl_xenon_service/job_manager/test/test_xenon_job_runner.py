from .context import simple_cwl_xenon_service

from simple_cwl_xenon_service.job_manager.xenon_job_runner import XenonJobRunner
from simple_cwl_xenon_service.job_manager.in_memory_job_store import InMemoryJobStore
from simple_cwl_xenon_service.job_manager.job_description import JobDescription

import os
import pytest

@pytest.fixture
def workflowfile(request):
    thisfile = request.module.__file__
    thisdir = os.path.dirname(thisfile)
    return os.path.join(thisdir, 'test_workflow.cwl')

def test_init():
    store = InMemoryJobStore()
    runner = XenonJobRunner(store)

def test_start_job(workflowfile):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store)

    test_job = JobDescription("TestJob", workflowfile, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

