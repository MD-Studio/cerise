from .context import simple_cwl_xenon_service

from simple_cwl_xenon_service.job_manager.xenon_job_runner import XenonJobRunner
from simple_cwl_xenon_service.job_manager.in_memory_job_store import InMemoryJobStore
from simple_cwl_xenon_service.job_manager.job_description import JobDescription
from simple_cwl_xenon_service.job_manager.job_state import JobState

import os
import pytest
import time
import xenon
import yaml

@pytest.fixture(scope="module")
def xenon(request):
    xenon.init()
    return None

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

@pytest.fixture
def xenon_config(request):
    thisfile = request.module.__file__
    thisdir = os.path.dirname(thisfile)
    test_config_file_path = os.path.join(thisdir, 'config.yml')
    with open(test_config_file_path) as test_config_file:
        test_config = yaml.load(test_config_file)
    return test_config['compute-resource']


def test_init(xenon_config):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store, xenon_config)

def test_start_job(workflowfile, xenon_config):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_start_job", workflowfile, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

def test_update(slowworkflow, xenon_config):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store, xenon_config)

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

def test_cancel(slowworkflow, xenon_config):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_cancel", slowworkflow, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

    time.sleep(2)

    runner.cancel_job(job_id)

    updated_job = store.get_job(job_id)
    assert updated_job.get_state() == JobState.CANCELLED

    runner.cancel_job(job_id)
    assert updated_job.get_state() == JobState.CANCELLED

def test_delete_running(slowworkflow, xenon_config):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_delete_running", slowworkflow, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

    time.sleep(2)

    runner.delete_job(job_id)
    # TODO: Should test that remote dir is gone somehow?
    # Needs better test setup I think

def test_delete_cancelled(slowworkflow, xenon_config):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store, xenon_config)

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

def test_delete_done(workflowfile, xenon_config):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_start_job", workflowfile, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

    while store.get_job(job_id).get_state == JobState.RUNNING:
        time.sleep(0.1)

    runner.delete_job(job_id)
    # TODO: Should test that remote dir is gone somehow?
    # Needs better test setup I think

def test_get_log(workflowfile, xenon_config):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_get_log", workflowfile, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

    while not JobState.is_done(store.get_job(job_id).get_state()):
        time.sleep(0.1)
        runner.update(job_id)

    log = store.get_job(job_id).get_log()

    assert len(log) > 0
    assert 'success' in log

def test_get_output(workflowfile, xenon_config):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_get_output", workflowfile, {})
    job_id = store.create_job(test_job)
    runner.start_job(job_id)

    while not JobState.is_done(store.get_job(job_id).get_state()):
        time.sleep(0.1)
        runner.update(job_id)

    output = store.get_job(job_id).get_output()

    assert len(output) > 0
    assert 'checksum' in output
