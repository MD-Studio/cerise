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


def test_stage_job(fixture):
    fixture['store'].add_test_job('test_stage_job', 'wc', 'resolved')
    input_files = fixture['store'].get_input_files('wc')
    fixture['remote-job-files'].stage_job('test_stage_job', input_files, bytes())

    remote_file = os.path.join(fixture['remote-dir'], 'jobs',
            'test_stage_job', 'work', '01_input_test_job_hello_world.txt')
    with open(remote_file, 'rb') as f:
        content = f.read()
        assert content == input_files[0].content

def test_stage_secondary_files(fixture):
    fixture['store'].add_test_job('test_stage_secondary_files', 'secondary_files', 'resolved')
    input_files = fixture['store'].get_input_files('secondary_files')
    fixture['remote-job-files'].stage_job('test_stage_secondary_files', input_files, bytes())

    remote_file = os.path.join(fixture['remote-dir'], 'jobs',
            'test_stage_secondary_files', 'work', '01_input_test_job_hello_world.txt')
    with open(remote_file, 'rb') as f:
        content = f.read()
        assert content == input_files[0].content

    remote_file = os.path.join(fixture['remote-dir'], 'jobs',
            'test_stage_secondary_files', 'work', '02_input_test_job_hello_world.2nd')
    with open(remote_file, 'rb') as f:
        content = f.read()
        assert content == input_files[0].secondary_files[0].content

def test_stage_file_array(fixture):
    fixture['store'].add_test_job('test_stage_file_array', 'file_array', 'resolved')
    input_files = fixture['store'].get_input_files('file_array')
    fixture['remote-job-files'].stage_job('test_stage_file_array', input_files, bytes())

    remote_file = os.path.join(fixture['remote-dir'], 'jobs',
            'test_stage_file_array', 'work', '01_input_test_job_hello_world.txt')
    with open(remote_file, 'rb') as f:
        content = f.read()
        assert content == input_files[0].content

    remote_file = os.path.join(fixture['remote-dir'], 'jobs',
            'test_stage_file_array', 'work', '02_input_test_job_hello_world.2nd')
    with open(remote_file, 'rb') as f:
        content = f.read()
        assert content == input_files[1].content

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
