from cerise.test.xenon import xenon_init

from .mock_store import MockStore
from cerise.test.fixture_jobs import WcJob

import os
import pytest
import xenon

@pytest.fixture
def x(request, xenon_init):
    ret = xenon.Xenon()
    yield ret
    ret.close()

class MockConfig:
    def __init__(self, x, remote_dir):
        self._x = x
        self._remote_dir = remote_dir

    def get_file_system(self):
        return self._x.files().newFileSystem('local', None, None, None)

    def get_basedir(self):
        return self._remote_dir

    def get_username(self, kind):
        return None

@pytest.fixture
def fixture(request, tmpdir, x):
    from cerise.back_end.xenon_remote_files import XenonRemoteFiles

    result = {}

    result['remote-dir'] = str(tmpdir)

    result['store'] = MockStore({
        'local-base-path': '',
        'remote-base-path': result['remote-dir']
        })

    result['xenon'] = x

    result['xenon-remote-files-config'] = MockConfig(result['xenon'], result['remote-dir'])

    result['xenon-remote-files'] = XenonRemoteFiles(
            result['store'], x, result['xenon-remote-files-config'])

    local_api_dir = os.path.join(os.path.dirname(__file__), 'api')
    result['xenon-remote-files'].stage_api(local_api_dir)

    return result


def test_stage_api(fixture):
    assert os.path.isfile(os.path.join(
        fixture['remote-dir'], 'api', 'steps', 'test', 'wc.cwl'))

def test_stage_job(fixture):
    fixture['store'].add_test_job('test_stage_job', 'wc', 'resolved')
    input_files = fixture['store'].get_input_files('wc')
    fixture['xenon-remote-files'].stage_job('test_stage_job', input_files)

    remote_file = os.path.join(fixture['remote-dir'], 'jobs',
            'test_stage_job', 'work', '01_input_hello_world.txt')
    with open(remote_file, 'rb') as f:
        content = f.read()
        assert content == input_files[0].content

def test_stage_secondary_files(fixture):
    fixture['store'].add_test_job('test_stage_secondary_files', 'secondary_files', 'resolved')
    input_files = fixture['store'].get_input_files('secondary_files')
    fixture['xenon-remote-files'].stage_job('test_stage_secondary_files', input_files)

    remote_file = os.path.join(fixture['remote-dir'], 'jobs',
            'test_stage_secondary_files', 'work', '01_input_hello_world.txt')
    with open(remote_file, 'rb') as f:
        content = f.read()
        assert content == input_files[0].content

    remote_file = os.path.join(fixture['remote-dir'], 'jobs',
            'test_stage_secondary_files', 'work', '02_input_hello_world.2nd')
    with open(remote_file, 'rb') as f:
        content = f.read()
        assert content == input_files[0].secondary_files[0].content


def test_destage_job_no_output(fixture):
    fixture['store'].add_test_job('test_destage_job_no_output', 'pass', 'run_and_updated')
    output_files = fixture['xenon-remote-files'].destage_job_output('test_destage_job_no_output')
    assert output_files == []

def test_destage_job_output(fixture):
    fixture['store'].add_test_job('test_destage_job_output', 'wc', 'run_and_updated')
    output_files = fixture['xenon-remote-files'].destage_job_output('test_destage_job_output')
    assert output_files == WcJob.output_files

def test_delete_job(fixture):
    job_dir = os.path.join(fixture['remote-dir'], 'jobs', 'test_delete_job')
    work_dir = os.path.join(job_dir, 'work')
    os.makedirs(work_dir)
    fixture['xenon-remote-files'].delete_job('test_delete_job')
    assert not os.path.exists(job_dir)

def test_update_job(fixture):
    fixture['store'].add_test_job('test_update_job', 'wc', 'run')
    fixture['xenon-remote-files'].update_job('test_update_job')
    wc_remote_workdir = os.path.join(fixture['remote-dir'], 'jobs', 'test_update_job', 'work')
    assert fixture['store'].get_job('test_update_job').remote_output == WcJob.remote_output('file://' + wc_remote_workdir)
    # check that we have the log?
