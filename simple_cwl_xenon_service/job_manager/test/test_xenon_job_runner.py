from .context import simple_cwl_xenon_service

from simple_cwl_xenon_service.job_manager.xenon_job_runner import XenonJobRunner
from simple_cwl_xenon_service.job_manager.xenon_remote_files import XenonRemoteFiles
from simple_cwl_xenon_service.job_manager.in_memory_job_store import InMemoryJobStore
from simple_cwl_xenon_service.job_manager.job_description import JobDescription
from simple_cwl_xenon_service.job_manager.job_state import JobState

import os
import pytest
import time
import xenon
import yaml

@pytest.fixture(scope="module")
def xenon_init(request):
    xenon.init()
    return None

@pytest.fixture
def x(request):
    ret = xenon.Xenon()
    yield ret
    ret.close()

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
def staging_workflow(request):
    thisfile = request.module.__file__
    thisdir = os.path.dirname(thisfile)
    return os.path.join(thisdir, 'staging_workflow.cwl')

@pytest.fixture
def staging_workflow_input(request):
    thisfile = request.module.__file__
    thisdir = os.path.dirname(thisfile)
    input_file_path = os.path.join(thisdir, 'staging_workflow_input.yml')
    with open(input_file_path) as input_file:
        input = yaml.load(input_file)
    return input

@pytest.fixture
def staging_workflow_inputfile(request):
    thisfile = request.module.__file__
    thisdir = os.path.dirname(thisfile)
    input_file = os.path.join(thisdir, 'hello_world.txt')
    return open(input_file, 'rb').read()

@pytest.fixture
def xenon_config(request):
    thisfile = request.module.__file__
    thisdir = os.path.dirname(thisfile)
    test_config_file_path = os.path.join(thisdir, 'config.yml')
    with open(test_config_file_path) as test_config_file:
        test_config = yaml.load(test_config_file)
    return test_config['compute-resource']


def test_init(x, xenon_config):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store, x, xenon_config)

def test_start_job(workflowfile, x, xenon_config):
    store = InMemoryJobStore()
    remote_files = XenonRemoteFiles(store, x, xenon_config)
    runner = XenonJobRunner(store, x, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_start_job", workflowfile, {})
    job_id = store.create_job(test_job)
    remote_files.stage_job(job_id, {})
    runner.start_job(job_id)

def test_start_staging_job(staging_workflow, staging_workflow_input, staging_workflow_inputfile, x, xenon_config):
    store = InMemoryJobStore()
    remote_files = XenonRemoteFiles(store, x, xenon_config)
    runner = XenonJobRunner(store, x, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_start_staging_job",
        staging_workflow, staging_workflow_input)
    job_id = store.create_job(test_job)
    remote_files.stage_job(job_id, {'file': staging_workflow_inputfile})
    runner.start_job(job_id)

    time.sleep(2)

    runner.update_job(job_id)
    updated_job = store.get_job(job_id)
    assert updated_job.get_state() == JobState.SUCCESS

def test_update(slowworkflow, x, xenon_config):
    store = InMemoryJobStore()
    remote_files = XenonRemoteFiles(store, x, xenon_config)
    runner = XenonJobRunner(store, x, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_update", slowworkflow, {})
    job_id = store.create_job(test_job)
    remote_files.stage_job(job_id, {})
    runner.start_job(job_id)

    time.sleep(2)

    runner.update_job(job_id)
    updated_job = store.get_job(job_id)
    assert updated_job.get_state() == JobState.RUNNING

    time.sleep(4)

    runner.update_job(job_id)
    updated_job = store.get_job(job_id)
    assert updated_job.get_state() == JobState.SUCCESS

def test_cancel(slowworkflow, x, xenon_config):
    store = InMemoryJobStore()
    remote_files = XenonRemoteFiles(store, x, xenon_config)
    runner = XenonJobRunner(store, x, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_cancel", slowworkflow, {})
    job_id = store.create_job(test_job)
    remote_files.stage_job(job_id, {})
    runner.start_job(job_id)

    time.sleep(2)

    runner.cancel_job(job_id)

    updated_job = store.get_job(job_id)
    assert updated_job.get_state() == JobState.CANCELLED

    runner.cancel_job(job_id)
    assert updated_job.get_state() == JobState.CANCELLED

def test_delete_running(slowworkflow, x, xenon_config):
    store = InMemoryJobStore()
    remote_files = XenonRemoteFiles(store, x, xenon_config)
    runner = XenonJobRunner(store, x, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_delete_running", slowworkflow, {})
    job_id = store.create_job(test_job)
    remote_files.stage_job(job_id, {})
    runner.start_job(job_id)

    time.sleep(2)

    runner.cancel_job(job_id)
    remote_files.delete_job(job_id)
    # TODO: Should test that remote dir is gone somehow?
    # Needs better test setup I think

def test_delete_cancelled(slowworkflow, x, xenon_config):
    store = InMemoryJobStore()
    remote_files = XenonRemoteFiles(store, x, xenon_config)
    runner = XenonJobRunner(store, x, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_cancel", slowworkflow, {})
    job_id = store.create_job(test_job)
    remote_files.stage_job(job_id, {})
    runner.start_job(job_id)

    time.sleep(2)

    runner.cancel_job(job_id)

    updated_job = store.get_job(job_id)
    assert updated_job.get_state() == JobState.CANCELLED

    remote_files.delete_job(job_id)
    # TODO: Should test that remote dir is gone somehow?
    # Needs better test setup I think

def test_delete_done(workflowfile, x, xenon_config):
    store = InMemoryJobStore()
    remote_files = XenonRemoteFiles(store, x, xenon_config)
    runner = XenonJobRunner(store, x, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_start_job", workflowfile, {})
    job_id = store.create_job(test_job)
    remote_files.stage_job(job_id, {})
    runner.start_job(job_id)

    while store.get_job(job_id).get_state == JobState.RUNNING:
        time.sleep(0.1)

    remote_files.delete_job(job_id)
    # TODO: Should test that remote dir is gone somehow?
    # Needs better test setup I think

def test_get_log(workflowfile, x, xenon_config):
    store = InMemoryJobStore()
    remote_files = XenonRemoteFiles(store, x, xenon_config)
    runner = XenonJobRunner(store, x, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_get_log", workflowfile, {})
    job_id = store.create_job(test_job)
    remote_files.stage_job(job_id, {})
    runner.start_job(job_id)

    while not JobState.is_done(store.get_job(job_id).get_state()):
        time.sleep(0.1)
        runner.update_job(job_id)

    remote_files.update_job(job_id)
    log = store.get_job(job_id).get_log()

    assert len(log) > 0
    assert 'success' in log

def test_get_output(workflowfile, x, xenon_config):
    store = InMemoryJobStore()
    remote_files = XenonRemoteFiles(store, x, xenon_config)
    runner = XenonJobRunner(store, x, xenon_config)

    test_job = JobDescription("test_xenon_job_runner.test_get_output", workflowfile, {})
    job_id = store.create_job(test_job)
    remote_files.stage_job(job_id, {})
    runner.start_job(job_id)

    while not JobState.is_done(store.get_job(job_id).get_state()):
        time.sleep(0.1)
        runner.update_job(job_id)

    remote_files.update_job(job_id)
    output = store.get_job(job_id).get_output()

    assert len(output) > 0
    assert 'checksum' in output
