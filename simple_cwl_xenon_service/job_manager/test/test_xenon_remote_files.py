from simple_cwl_xenon_service.job_manager.xenon_remote_files import XenonRemoteFiles

from .mock_store import MockStore
from .fixture_jobs import WcJob

import os
import pytest
import xenon

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
def fixture(request, tmpdir, x):
    result = {}

    result['remote-dir'] = str(tmpdir)

    result['store'] = MockStore({
        'local-base-path': '',
        'remote-base-path': result['remote-dir']
        })

    result['xenon'] = x

    result['xenon-remote-files-config'] = {
            'files': {
                'scheme': 'local',
                'location': None,
                'path': result['remote-dir'],
                'credential': None,
                'properties': None
            }}

    result['xenon-remote-files'] = XenonRemoteFiles(
            result['store'], x, result['xenon-remote-files-config'])

    return result


def test_init(fixture):
    pass

def test_stage_job(fixture):
    fixture['store'].add_test_job('test_stage_job', 'wc', 'resolved')
    input_files = fixture['store'].get_input_files('wc')
    fixture['xenon-remote-files'].stage_job('test_stage_job', input_files)

    remote_file = os.path.join(fixture['remote-dir'], 'jobs',
            'test_stage_job', 'work', '01_input_hello_world.txt')
    with open(remote_file, 'rb') as f:
        contents = f.read()
        assert contents == input_files[0][2]

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
