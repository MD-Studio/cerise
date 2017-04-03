from .context import simple_cwl_xenon_service

from simple_cwl_xenon_service.job_manager.xenon_job_runner import XenonJobRunner
from simple_cwl_xenon_service.job_manager.local_files import LocalFiles
from simple_cwl_xenon_service.job_manager.xenon_remote_files import XenonRemoteFiles
from simple_cwl_xenon_service.job_manager.in_memory_job_store import InMemoryJobStore
from simple_cwl_xenon_service.job_manager.job_description import JobDescription
from simple_cwl_xenon_service.job_manager.job_state import JobState

import json
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
    return 'input/test_workflow.cwl'

@pytest.fixture
def slowworkflow(request):
    return 'input/slow_workflow.cwl'

@pytest.fixture
def staging_workflow(request):
    return 'input/staging_workflow.cwl'

@pytest.fixture
def staging_workflow_input(request):
    thisfile = request.module.__file__
    thisdir = os.path.dirname(thisfile)
    input_file_path = os.path.join(thisdir, 'files/input/staging_workflow_input.json')
    with open(input_file_path, 'r') as input_file:
        input = input_file.read()
    return input

@pytest.fixture
def config(request):
    thisfile = request.module.__file__
    thisdir = os.path.dirname(thisfile)
    test_config_file_path = os.path.join(thisdir, 'config.yml')
    with open(test_config_file_path) as test_config_file:
        test_config = yaml.load(test_config_file)

    test_config['local']['file-store-path'] = thisdir + '/files'
    return test_config


def test_init(x, config):
    store = InMemoryJobStore()
    runner = XenonJobRunner(store, x, config['compute-resource'])

def test_start_job(workflowfile, x, config):
    store = InMemoryJobStore()
    local_files = LocalFiles(store, config['local'])
    remote_files = XenonRemoteFiles(store, x, config['compute-resource'])
    runner = XenonJobRunner(store, x, config['compute-resource'])

    test_job = JobDescription("test_xenon_job_runner.test_start_job", workflowfile, {})
    job_id = store.create_job(test_job)
    local_files.resolve_input(job_id)
    remote_files.stage_job(job_id)
    runner.start_job(job_id)

def test_start_staging_job(staging_workflow, staging_workflow_input, x, config):
    store = InMemoryJobStore()
    local_files = LocalFiles(store, config['local'])
    remote_files = XenonRemoteFiles(store, x, config['compute-resource'])
    runner = XenonJobRunner(store, x, config['compute-resource'])

    test_job = JobDescription("test_xenon_job_runner.test_start_staging_job",
        staging_workflow, staging_workflow_input)
    job_id = store.create_job(test_job)
    local_files.resolve_input(job_id)
    remote_files.stage_job(job_id)
    runner.start_job(job_id)

    time.sleep(2)

    runner.update_job(job_id)
    updated_job = store.get_job(job_id)
    assert updated_job.state == JobState.SUCCESS

def test_update(slowworkflow, x, config):
    store = InMemoryJobStore()
    local_files = LocalFiles(store, config['local'])
    remote_files = XenonRemoteFiles(store, x, config['compute-resource'])
    runner = XenonJobRunner(store, x, config['compute-resource'])

    test_job = JobDescription("test_xenon_job_runner.test_update", slowworkflow, {})
    job_id = store.create_job(test_job)
    local_files.resolve_input(job_id)
    remote_files.stage_job(job_id,)
    runner.start_job(job_id)

    time.sleep(2)

    runner.update_job(job_id)
    updated_job = store.get_job(job_id)
    assert updated_job.state == JobState.RUNNING

    time.sleep(4)

    runner.update_job(job_id)
    updated_job = store.get_job(job_id)
    assert updated_job.state == JobState.SUCCESS

def test_cancel(slowworkflow, x, config):
    store = InMemoryJobStore()
    local_files = LocalFiles(store, config['local'])
    remote_files = XenonRemoteFiles(store, x, config['compute-resource'])
    runner = XenonJobRunner(store, x, config['compute-resource'])

    test_job = JobDescription("test_xenon_job_runner.test_cancel", slowworkflow, {})
    job_id = store.create_job(test_job)
    local_files.resolve_input(job_id)
    remote_files.stage_job(job_id)
    runner.start_job(job_id)

    time.sleep(2)

    runner.cancel_job(job_id)

    updated_job = store.get_job(job_id)
    assert updated_job.state == JobState.CANCELLED

    runner.cancel_job(job_id)
    assert updated_job.state == JobState.CANCELLED

def test_delete_running(slowworkflow, x, config):
    store = InMemoryJobStore()
    local_files = LocalFiles(store, config['local'])
    remote_files = XenonRemoteFiles(store, x, config['compute-resource'])
    runner = XenonJobRunner(store, x, config['compute-resource'])

    test_job = JobDescription("test_xenon_job_runner.test_delete_running", slowworkflow, {})
    job_id = store.create_job(test_job)
    local_files.resolve_input(job_id)
    remote_files.stage_job(job_id)
    runner.start_job(job_id)

    time.sleep(2)

    runner.cancel_job(job_id)
    remote_files.delete_job(job_id)
    # TODO: Should test that remote dir is gone somehow?
    # Needs better test setup I think

def test_delete_cancelled(slowworkflow, x, config):
    store = InMemoryJobStore()
    local_files = LocalFiles(store, config['local'])
    remote_files = XenonRemoteFiles(store, x, config['compute-resource'])
    runner = XenonJobRunner(store, x, config['compute-resource'])

    test_job = JobDescription("test_xenon_job_runner.test_cancel", slowworkflow, {})
    job_id = store.create_job(test_job)
    local_files.resolve_input(job_id)
    remote_files.stage_job(job_id)
    runner.start_job(job_id)

    time.sleep(2)

    runner.cancel_job(job_id)

    updated_job = store.get_job(job_id)
    assert updated_job.state == JobState.CANCELLED

    remote_files.delete_job(job_id)
    # TODO: Should test that remote dir is gone somehow?
    # Needs better test setup I think

def test_delete_done(workflowfile, x, config):
    store = InMemoryJobStore()
    local_files = LocalFiles(store, config['local'])
    remote_files = XenonRemoteFiles(store, x, config['compute-resource'])
    runner = XenonJobRunner(store, x, config['compute-resource'])

    test_job = JobDescription("test_xenon_job_runner.test_start_job", workflowfile, {})
    job_id = store.create_job(test_job)
    local_files.resolve_input(job_id)
    remote_files.stage_job(job_id)
    runner.start_job(job_id)

    while store.get_job(job_id).state == JobState.RUNNING:
        time.sleep(0.1)

    remote_files.delete_job(job_id)
    # TODO: Should test that remote dir is gone somehow?
    # Needs better test setup I think

def test_get_log(workflowfile, x, config):
    store = InMemoryJobStore()
    local_files = LocalFiles(store, config['local'])
    remote_files = XenonRemoteFiles(store, x, config['compute-resource'])
    runner = XenonJobRunner(store, x, config['compute-resource'])

    test_job = JobDescription("test_xenon_job_runner.test_get_log", workflowfile, {})
    job_id = store.create_job(test_job)
    local_files.resolve_input(job_id)
    remote_files.stage_job(job_id)
    runner.start_job(job_id)

    while not JobState.is_done(store.get_job(job_id).state):
        time.sleep(0.1)
        runner.update_job(job_id)

    remote_files.update_job(job_id)
    log = store.get_job(job_id).log

    assert len(log) > 0
    assert 'success' in log

def test_get_output(workflowfile, x, config):
    store = InMemoryJobStore()
    local_files = LocalFiles(store, config['local'])
    remote_files = XenonRemoteFiles(store, x, config['compute-resource'])
    runner = XenonJobRunner(store, x, config['compute-resource'])

    test_job = JobDescription("test_xenon_job_runner.test_get_output", workflowfile, {})
    job_id = store.create_job(test_job)
    local_files.resolve_input(job_id)
    remote_files.stage_job(job_id)
    runner.start_job(job_id)

    while not JobState.is_done(store.get_job(job_id).state):
        time.sleep(0.1)
        runner.update_job(job_id)

    remote_files.update_job(job_id)
    output = store.get_job(job_id).output

    assert len(output) > 0
    assert 'checksum' in output
