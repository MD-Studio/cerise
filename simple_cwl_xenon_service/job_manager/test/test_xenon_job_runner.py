from .context import simple_cwl_xenon_service

from simple_cwl_xenon_service.job_manager.xenon_job_runner import XenonJobRunner
from simple_cwl_xenon_service.job_manager.in_memory_job_store import InMemoryJobStore
from simple_cwl_xenon_service.job_manager.job_description import JobDescription
from simple_cwl_xenon_service.job_manager.job_state import JobState

import os
import pytest
import time

@pytest.fixture
def workflowfile(request):
    thisfile = request.module.__file__
    thisdir = os.path.dirname(thisfile)
    return os.path.join(thisdir, 'test_workflow.cwl')

@pytest.fixture
def slowworkflow(request):
    thisfile = request.module.__file__
    thisdir = os.path.dirname(thisfile)
    return os.path.join(thisdir, 'slow_workflow.cwl')


def test_init():
    store = InMemoryJobStore()
    runner = XenonJobRunner(store)

def test_start_job(workflowfile):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store)

    test_job = JobDescription("TestJob", workflowfile, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

def test_update(slowworkflow):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store)

    test_job = JobDescription("TestJob", slowworkflow, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

    time.sleep(2)

    runner.update(job_id)
    updated_job = store.get_job(job_id)
    assert updated_job.get_state() == JobState.RUNNING

    time.sleep(4)

    runner.update(job_id)
    updated_job = store.get_job(job_id)
    assert updated_job.get_state() == JobState.SUCCESS

def test_cancel(slowworkflow):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store)

    test_job = JobDescription("TestJob", slowworkflow, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

    time.sleep(2)

    runner.cancel_job(job_id)

    updated_job = store.get_job(job_id)
    assert updated_job.get_state() == JobState.CANCELLED
