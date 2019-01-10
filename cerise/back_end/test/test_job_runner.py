import json
import pathlib
import shutil
import time

import pytest
import yaml

from cerise.back_end.job_runner import JobRunner
from cerise.job_store.job_state import JobState
from cerise.test.fixture_jobs import BrokenJob


def _stage_test_api(local_api_dir, remote_api_dir):
    """Copies an API to the mock remote dir for testing.
    """
    shutil.copytree(str(local_api_dir), str(remote_api_dir))

    for step in (remote_api_dir / 'test' / 'steps' / 'test').iterdir():
        if step.suffix == '.cwl':
            step_dict = yaml.safe_load(step.read_text())
            step.write_text(json.dumps(step_dict))

    cwltiny = pathlib.Path(
        __file__).parents[3] / 'api' / 'cerise' / 'files' / 'cwltiny.py'
    shutil.copy(
        str(cwltiny), str(remote_api_dir / 'cerise' / 'files' / 'cwltiny.py'))


@pytest.fixture
def runner_store(mock_config, mock_store_staged, local_api_dir):
    store, job_fixture = mock_store_staged

    remote_api_dir = mock_config.get_basedir() / 'api'
    _stage_test_api(local_api_dir, remote_api_dir)

    runner_path = remote_api_dir / 'cerise' / 'files' / 'cwltiny.py'
    job_runner = JobRunner(store, mock_config, str(runner_path))

    return job_runner, store, job_fixture


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


def test_start_job(runner_store, mock_config):
    job_runner, store, job_fixture = runner_store

    job_runner.start_job('test_job')
    store.get_job('test_job').state = JobState.WAITING

    _wait_for_state(store, job_runner, JobState.FINISHED, 5.0)

    logfile = mock_config.get_basedir() / 'jobs' / 'test_job' / 'stderr.txt'
    if job_fixture is not BrokenJob:
        print(logfile.read_text())
        assert 'Final process status is success' in logfile.read_text()


def test_update(runner_store):
    job_runner, store, _ = runner_store

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
    job_runner, store, _ = runner_store

    start_time = time.perf_counter()
    job_runner.start_job('test_job')
    store.get_job('test_job').state = JobState.WAITING

    _wait_for_state(store, job_runner, JobState.RUNNING, 2.0)

    is_running = job_runner.cancel_job('test_job')
    assert time.perf_counter() < start_time + 1.0
    assert is_running is False
    assert store.get_job('test_job').state == JobState.RUNNING

    is_running = job_runner.cancel_job('test_job')
    assert is_running is False
    assert store.get_job('test_job').state == JobState.RUNNING
