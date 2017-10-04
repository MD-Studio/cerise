from cerise.back_end.xenon_job_runner import XenonJobRunner
from cerise.job_store.job_state import JobState
from cerise.test.xenon import xenon_init

from .mock_store import MockStore

import os
import pytest
import shutil
import time
import xenon

@pytest.fixture
def x(request, xenon_init):
    ret = xenon.Xenon()
    yield ret
    ret.close()

@pytest.fixture
def fixture(request, tmpdir, x):
    result = {}

    result['remote-dir'] = str(tmpdir)
    result['store'] = MockStore({
        'local-base-path': '',
        'remote-base-path': result['remote-dir']
        })
    result['xenon'] = x
    result['xenon-job-runner-config'] = {
            'jobs': {
                'scheme': 'local',
                'location': None,
                'credential': None,
                'properties': None
            }}

    # stage api
    base_api_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'api')
    remote_api_dir = os.path.join(result['remote-dir'], 'api')
    shutil.copytree(base_api_dir, remote_api_dir)

    test_steps_dir = os.path.join(os.path.dirname(__file__), 'api', 'steps', 'test')
    remote_test_steps_dir = os.path.join(result['remote-dir'], 'api', 'steps', 'test')
    shutil.copytree(test_steps_dir, remote_test_steps_dir)

    test_install_script_dir = os.path.join(os.path.dirname(__file__), 'api', 'install.sh')
    remote_install_script_dir = os.path.join(result['remote-dir'], 'api', 'install.sh')
    shutil.copy2(test_install_script_dir, remote_install_script_dir)

    result['xenon-job-runner'] = XenonJobRunner(
            result['store'], x, result['xenon-job-runner-config'],
            result['remote-dir'] + '/api/files',
            result['remote-dir'] + '/api/install.sh')
    return result

def test_stage_api_script_execution(fixture):
    assert os.path.isfile(os.path.join(
        fixture['remote-dir'], 'api', 'files', 'test', 'test_file.txt'))

def test_start_job(fixture):
    fixture['store'].add_test_job('test_start_job', 'pass', 'staged')
    fixture['xenon-job-runner'].start_job('test_start_job')
    fixture['store'].get_job('test_start_job').state = JobState.WAITING

    time.sleep(1)

    fixture['xenon-job-runner'].update_job('test_start_job')
    updated_job = fixture['store'].get_job('test_start_job')
    assert updated_job.remote_job_id == 'local-1'
    assert updated_job.state == JobState.FINISHED

def test_start_staging_job(fixture):
    fixture['store'].add_test_job('test_start_staging_job', 'wc', 'staged')
    fixture['xenon-job-runner'].start_job('test_start_staging_job')
    fixture['store'].get_job('test_start_staging_job').state = JobState.WAITING

    time.sleep(2)

    fixture['xenon-job-runner'].update_job('test_start_staging_job')
    updated_job = fixture['store'].get_job('test_start_staging_job')
    assert updated_job.remote_job_id == 'local-1'
    assert updated_job.state == JobState.FINISHED

def test_start_broken_job(fixture):
    fixture['store'].add_test_job('test_start_broken_job', 'broken', 'staged')
    fixture['xenon-job-runner'].start_job('test_start_broken_job')
    fixture['store'].get_job('test_start_broken_job').state = JobState.WAITING

    time.sleep(1)

    fixture['xenon-job-runner'].update_job('test_start_broken_job')
    updated_job = fixture['store'].get_job('test_start_broken_job')
    assert updated_job.remote_job_id == 'local-1'
    assert updated_job.state == JobState.FINISHED
    assert updated_job.remote_output == ''

def test_update(fixture):
    fixture['store'].add_test_job('test_update', 'slow', 'staged')
    fixture['xenon-job-runner'].start_job('test_update')
    fixture['store'].get_job('test_update').state = JobState.WAITING

    time.sleep(2)

    fixture['xenon-job-runner'].update_job('test_update')
    updated_job = fixture['store'].get_job('test_update')
    assert updated_job.state == JobState.RUNNING

    time.sleep(4)

    fixture['xenon-job-runner'].update_job('test_update')
    updated_job = fixture['store'].get_job('test_update')
    assert updated_job.state == JobState.FINISHED

def test_cancel(fixture):
    fixture['store'].add_test_job('test_cancel', 'slow', 'staged')
    fixture['xenon-job-runner'].start_job('test_cancel')
    fixture['store'].get_job('test_cancel').state = JobState.WAITING

    time.sleep(2)
    is_running = fixture['xenon-job-runner'].cancel_job('test_cancel')
    assert is_running == False

    is_running = fixture['xenon-job-runner'].cancel_job('test_cancel')
    assert is_running == False
