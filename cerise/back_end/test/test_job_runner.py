from cerise.back_end.job_runner import JobRunner
from cerise.job_store.job_state import JobState

from .mock_store import MockStore

import cerulean
import os
import pathlib
import pytest
import shutil
import time


class MockConfig:
    def get_scheduler(self, run_on_head_node=False):
        term = cerulean.LocalTerminal()
        return cerulean.DirectGnuScheduler(term)

    def get_queue_name(self):
        return None

    def get_slots_per_node(self):
        return 1

    def get_cores_per_node(self):
        return 16

    def get_scheduler_options(self):
        return None

    def get_remote_cwl_runner(self):
        return '$CERISE_API/cerise/files/cwltiny.py'

@pytest.fixture
def fixture(request, tmpdir):
    result = {}

    result['remote-dir'] = str(tmpdir)
    result['store'] = MockStore({
        'local-base-path': '',
        'remote-base-path': result['remote-dir']
        })
    result['job-runner-config'] = MockConfig()

    # stage api
    base_api_dir = pathlib.Path(__file__).parents[3] / 'api'
    remote_api_dir = pathlib.Path(result['remote-dir']) / 'api'
    shutil.copytree(str(base_api_dir), str(remote_api_dir))

    test_api_dir = pathlib.Path(__file__).parent / 'api'
    shutil.copytree(str(test_api_dir / 'test'), str(remote_api_dir / 'test'))

    result['job-runner'] = JobRunner(
            result['store'], result['job-runner-config'],
            str(remote_api_dir) + '/cerise/files/cwltiny.py')
    return result

def _wait_for_state(fixture, job_id, state, timeout):
    """Waits for the job to be in the given state."""
    job = fixture['store'].get_job(job_id)
    total_time = 0.0
    while job.state != state and total_time < timeout:
        time.sleep(0.1)
        fixture['job-runner'].update_job(job_id)
        job = fixture['store'].get_job(job_id)
        total_time += 0.1

    assert total_time < timeout
    return job

def test_start_job(fixture):
    fixture['store'].add_test_job('test_start_job', 'pass', 'staged')
    fixture['job-runner'].start_job('test_start_job')
    fixture['store'].get_job('test_start_job').state = JobState.WAITING

    updated_job = _wait_for_state(fixture, 'test_start_job', JobState.FINISHED, 1.0)
    assert updated_job.remote_job_id != ''

def test_start_staging_job(fixture):
    fixture['store'].add_test_job('test_start_staging_job', 'wc', 'staged')
    fixture['job-runner'].start_job('test_start_staging_job')
    fixture['store'].get_job('test_start_staging_job').state = JobState.WAITING

    updated_job = _wait_for_state(fixture, 'test_start_staging_job', JobState.FINISHED, 2.0)
    assert updated_job.remote_job_id != ''

def test_start_broken_job(fixture):
    fixture['store'].add_test_job('test_start_broken_job', 'broken', 'staged')
    fixture['job-runner'].start_job('test_start_broken_job')
    fixture['store'].get_job('test_start_broken_job').state = JobState.WAITING

    updated_job = _wait_for_state(fixture, 'test_start_broken_job', JobState.FINISHED, 1.0)
    assert updated_job.remote_job_id != ''
    assert updated_job.remote_output == ''

def test_update(fixture):
    fixture['store'].add_test_job('test_update', 'slow', 'staged')
    fixture['job-runner'].start_job('test_update')
    fixture['store'].get_job('test_update').state = JobState.WAITING

    updated_job = _wait_for_state(fixture, 'test_update', JobState.RUNNING, 2.0)
    updated_job = _wait_for_state(fixture, 'test_update', JobState.FINISHED, 6.0)

def test_cancel(fixture):
    fixture['store'].add_test_job('test_cancel', 'slow', 'staged')
    fixture['job-runner'].start_job('test_cancel')
    fixture['store'].get_job('test_cancel').state = JobState.WAITING

    updated_job = _wait_for_state(fixture, 'test_cancel', JobState.RUNNING, 2.0)
    is_running = fixture['job-runner'].cancel_job('test_cancel')
    assert is_running == False

    is_running = fixture['job-runner'].cancel_job('test_cancel')
    assert is_running == False
