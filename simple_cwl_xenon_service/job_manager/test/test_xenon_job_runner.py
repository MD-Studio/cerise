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

    test_job = JobDescription("test_xenon_job_runner.test_start_job", workflowfile, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

def test_update(slowworkflow):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store)

    test_job = JobDescription("test_xenon_job_runner.test_update", slowworkflow, {})
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

    test_job = JobDescription("test_xenon_job_runner.test_cancel", slowworkflow, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

    time.sleep(2)

    runner.cancel_job(job_id)

    updated_job = store.get_job(job_id)
    assert updated_job.get_state() == JobState.CANCELLED

    runner.cancel_job(job_id)
    assert updated_job.get_state() == JobState.CANCELLED

def test_delete_running(slowworkflow):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store)

    test_job = JobDescription("test_xenon_job_runner.test_delete_running", slowworkflow, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

    time.sleep(2)

    runner.delete_job(job_id)
    # TODO: Should test that remote dir is gone somehow?
    # Needs better test setup I think

def test_delete_cancelled(slowworkflow):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store)

    test_job = JobDescription("test_xenon_job_runner.test_cancel", slowworkflow, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

    time.sleep(2)

    runner.cancel_job(job_id)

    updated_job = store.get_job(job_id)
    assert updated_job.get_state() == JobState.CANCELLED

    runner.delete_job(job_id)
    # TODO: Should test that remote dir is gone somehow?
    # Needs better test setup I think

def test_delete_done(workflowfile):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store)

    test_job = JobDescription("test_xenon_job_runner.test_start_job", workflowfile, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

    while store.get_job(job_id).get_state == JobState.RUNNING:
        time.sleep(0.1)

    runner.delete_job(job_id)
    # TODO: Should test that remote dir is gone somehow?
    # Needs better test setup I think
