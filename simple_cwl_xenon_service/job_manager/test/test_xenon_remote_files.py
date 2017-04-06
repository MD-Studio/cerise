from .context import simple_cwl_xenon_service

from simple_cwl_xenon_service.job_manager.xenon_remote_files import XenonRemoteFiles

from .mock_store import MockStore

import json
import os
import pytest
import shutil
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

@pytest.fixture(scope="module")
def remote_dir(request):
    thisfile = request.module.__file__
    thisdir = os.path.dirname(thisfile)
    basedir = os.path.join(thisdir, 'fixture', 'xenon_remote_files')
    if os.path.exists(basedir):
        shutil.rmtree(basedir)
    os.mkdir(basedir)
    yield basedir
    shutil.rmtree(basedir)

@pytest.fixture
def fixture(request, remote_dir, x):
    result = {}

    result['remote-dir'] = remote_dir

    result['store'] = MockStore({
        'remote-base-path': remote_dir
        })

    result['xenon'] = x

    result['xenon-remote-files-config'] = {
            'files': {
                'scheme': 'local',
                'location': None,
                'path': remote_dir,
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
    fixture['xenon-remote-files'].stage_job('test_stage_job')

    remote_file = os.path.join(fixture['remote-dir'], 'jobs',
            'test_stage_job', 'work', '01_input_hello_world.txt')
    with open(remote_file, 'rb') as f:
        contents = f.read()
        assert contents == fixture['store'].get_job('test_stage_job').input_files[0][2]


