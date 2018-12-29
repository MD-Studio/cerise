from .mock_store import MockStore
from cerise.back_end.test.fixture_jobs import WcJob
from cerise.back_end.remote_job_files import RemoteJobFiles
from cerise.back_end.remote_api import RemoteApi

import cerulean

import os
import pytest


@pytest.fixture
def fixture(request, mock_config):
    result = {}

    result['remote-files-config'] = mock_config
    result['remote-dir'] = str(mock_config.get_basedir())

    result['store'] = MockStore({
        'local-base-path': '',
        'remote-base-path': result['remote-dir']
        })

    local_api_dir = os.path.join(os.path.dirname(__file__), 'api')
    result['remote-api'] = RemoteApi(
            result['remote-files-config'], local_api_dir)

    result['remote-api'].install()

    result['remote-job-files'] = RemoteJobFiles(
            result['store'], result['remote-files-config'])

    return result


def test_new_stage_job(mock_config, mock_store_resolved):
    store, job_fixture = mock_store_resolved

    remote_job_files = RemoteJobFiles(store, mock_config)

    input_files = job_fixture.local_input_files
    remote_job_files.stage_job('test_job', input_files, job_fixture.workflow)

    remote_base = mock_config.get_basedir()
    jobdir = remote_base / 'jobs' / 'test_job'
    for _, path, content in job_fixture.remote_input_files:
        staged_file = jobdir / 'work' / path
        assert staged_file.read_bytes() == content


def test_destage_job_no_output(fixture):
    fixture['store'].add_test_job('test_destage_job_no_output', 'pass', 'run_and_updated')
    output_files = fixture['remote-job-files'].destage_job_output('test_destage_job_no_output')
    assert output_files == []

def test_destage_job_output(fixture):
    fixture['store'].add_test_job('test_destage_job_output', 'wc', 'run_and_updated')
    output_files = fixture['remote-job-files'].destage_job_output('test_destage_job_output')
    assert output_files == WcJob.output_files

def test_delete_job(fixture):
    job_dir = os.path.join(fixture['remote-dir'], 'jobs', 'test_delete_job')
    work_dir = os.path.join(job_dir, 'work')
    os.makedirs(work_dir)
    fixture['remote-job-files'].delete_job('test_delete_job')
    assert not os.path.exists(job_dir)

def test_update_job(fixture):
    fixture['store'].add_test_job('test_update_job', 'wc', 'run')
    fixture['remote-job-files'].update_job('test_update_job')
    wc_remote_workdir = os.path.join(fixture['remote-dir'], 'jobs', 'test_update_job', 'work')
    assert fixture['store'].get_job('test_update_job').remote_output == WcJob.remote_output('file://' + wc_remote_workdir)
    # check that we have the log?
