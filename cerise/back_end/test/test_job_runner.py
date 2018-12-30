from cerise.back_end.job_runner import JobRunner
from cerise.job_store.job_state import JobState

import pathlib
import pytest
import shutil
import time


def _stage_test_api(remote_api_dir):
    """Copies an API to the mock remote dir for testing.
    """
    base_api_dir = pathlib.Path(__file__).parents[3] / 'api'
    shutil.copytree(str(base_api_dir), str(remote_api_dir))

    test_api_dir = pathlib.Path(__file__).parent / 'api'
    shutil.copytree(str(test_api_dir / 'test'), str(remote_api_dir / 'test'))


@pytest.fixture
def runner_store(mock_config, mock_store_staged):
    store, job_fixture = mock_store_staged

    remote_api_dir = mock_config.get_basedir() / 'api'
    _stage_test_api(remote_api_dir)

    runner_path = remote_api_dir / 'cerise' / 'files' / 'cwltiny.py'
    job_runner = JobRunner(store, mock_config, str(runner_path))

    return job_runner, store


def _wait_for_state(store, job_runner, state, timeout):
    """Waits for the job to be in the given state.
    """
    job = store.get_job('test_job')
    total_time = 0.0
    while job.state != state and total_time < timeout:
        time.sleep(0.01)
        job_runner.update_job('test_job')
        job = store.get_job('test_job')
        total_time += 0.01

    assert total_time < timeout
    return job


def test_start_job(runner_store):
    job_runner, store = runner_store

    job_runner.start_job('test_job')
    store.get_job('test_job').state = JobState.WAITING

    _wait_for_state(store, job_runner, JobState.FINISHED, 5.0)


def test_update(runner_store):
    job_runner, store = runner_store

    job_runner.start_job('test_job')
    store.get_job('test_job').state = JobState.WAITING

    job = store.get_job('test_job')
    cur_state = JobState.WAITING
    states = [cur_state]
    while cur_state != JobState.FINISHED:
        job_runner.update_job('test_job')
        job = store.get_job('test_job')
        if job.state != cur_state:
            cur_state = job.state
            states.append(cur_state)

    i = 0
    if states[i] == JobState.WAITING:
        i += 1
    if states[i] == JobState.RUNNING:
        i += 1
    if states[i] == JobState.FINISHED:
        i += 1
    assert i == len(states)


def test_cancel(runner_store):
    job_runner, store = runner_store

    job_runner.start_job('test_job')
    store.get_job('test_job').state = JobState.WAITING

    updated_job = _wait_for_state(store, job_runner, JobState.RUNNING, 2.0)

    is_running = job_runner.cancel_job('test_job')
    assert is_running == False
    assert store.get_job('test_job').state == JobState.RUNNING

    is_running = job_runner.cancel_job('test_job')
    assert is_running == False
    assert store.get_job('test_job').state == JobState.RUNNING
