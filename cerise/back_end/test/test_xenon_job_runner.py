from cerise.back_end.xenon_job_runner import XenonJobRunner
from cerise.job_store.job_state import JobState
from cerise.test.xenon import xenon_init

from .mock_store import MockStore

import pytest
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
    result['xenon-job-runner'] = XenonJobRunner(
            result['store'], x, result['xenon-job-runner-config'])
    return result

def test_init(fixture):
    pass

def test_start_job(fixture):
    fixture['store'].add_test_job('test_start_job', 'pass', 'staged')
    fixture['xenon-job-runner'].start_job('test_start_job')
    fixture['store'].get_job('test_start_job').state = JobState.WAITING

    time.sleep(1)

    fixture['xenon-job-runner'].update_job('test_start_job')
    updated_job = fixture['store'].get_job('test_start_job')
    assert updated_job.remote_job_id == 'local-0'
    assert updated_job.state == JobState.FINISHED

def test_start_staging_job(fixture):
    fixture['store'].add_test_job('test_start_staging_job', 'wc', 'staged')
    fixture['xenon-job-runner'].start_job('test_start_staging_job')
    fixture['store'].get_job('test_start_staging_job').state = JobState.WAITING

    time.sleep(2)

    fixture['xenon-job-runner'].update_job('test_start_staging_job')
    updated_job = fixture['store'].get_job('test_start_staging_job')
    assert updated_job.remote_job_id == 'local-0'
    assert updated_job.state == JobState.FINISHED

def test_start_broken_job(fixture):
    fixture['store'].add_test_job('test_start_broken_job', 'broken', 'staged')
    fixture['xenon-job-runner'].start_job('test_start_broken_job')
    fixture['store'].get_job('test_start_broken_job').state = JobState.WAITING

    time.sleep(1)

    fixture['xenon-job-runner'].update_job('test_start_broken_job')
    updated_job = fixture['store'].get_job('test_start_broken_job')
    assert updated_job.remote_job_id == 'local-0'
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
