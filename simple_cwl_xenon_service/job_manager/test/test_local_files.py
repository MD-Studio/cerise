from .context import simple_cwl_xenon_service

from simple_cwl_xenon_service.job_manager.local_files import LocalFiles
from .mock_store import MockStore

from .fixture_jobs import PassJob
from .fixture_jobs import WcJob

import json
import os
import pytest
import shutil
import time
import yaml

@pytest.fixture
def fixture(request, tmpdir):
    result = {}

    basedir = str(tmpdir)

    result['output-dir'] = os.path.join(basedir, 'output')

    result['store'] = MockStore({
        'local-base-path': basedir
        })

    result['local-files-config'] = {
        'file-store-path': basedir,
        'file-store-location': 'http://example.com'
        }

    result['local-files'] = LocalFiles(result['store'], result['local-files-config'])

    return result

def test_init(fixture):
    pass

def test_resolve_no_input(fixture):
    fixture['store'].add_test_job('test_resolve_no_input', 'pass', 'submitted')
    fixture['local-files'].resolve_input('test_resolve_no_input')
    assert fixture['store'].get_job('test_resolve_no_input').workflow_content == PassJob.workflow

def test_resolve_input(fixture):
    fixture['store'].add_test_job('test_resolve_input', 'wc', 'submitted')
    fixture['local-files'].resolve_input('test_resolve_input')
    assert fixture['store'].get_job('test_resolve_input').workflow_content == WcJob.workflow
    assert fixture['store'].get_job('test_resolve_input').input_files == WcJob.input_files

def test_resolve_missing_input(fixture):
    fixture['store'].add_test_job('test_missing_input', 'missing_input', 'submitted')
    with pytest.raises(FileNotFoundError):
        fixture['local-files'].resolve_input('test_missing_input')

def test_create_output_dir(fixture):
    fixture['local-files'].create_output_dir('test_create_output_dir')
    output_dir_ref = os.path.join(fixture['output-dir'], 'test_create_output_dir')
    assert os.path.isdir(output_dir_ref)

def test_delete_output_dir(fixture):
    output_dir = os.path.join(fixture['output-dir'], 'test_delete_output_dir')
    os.mkdir(output_dir)
    fixture['local-files'].delete_output_dir('test_delete_output_dir')
    assert not os.path.exists(output_dir)

def test_publish_no_output(fixture):
    fixture['store'].add_test_job('test_publish_no_output', 'pass', 'submitted')
    output_dir = os.path.join(fixture['output-dir'], 'test_publish_no_output')
    os.mkdir(output_dir)
    fixture['local-files'].publish_job_output('test_publish_no_output')
    assert os.listdir(output_dir) == []

def test_publish_output(fixture):
    fixture['store'].add_test_job('test_publish_output', 'wc', 'destaged')
    output_dir = os.path.join(fixture['output-dir'], 'test_publish_output')
    os.mkdir(output_dir)
    fixture['local-files'].publish_job_output('test_publish_output')
    output_path = os.path.join(output_dir, 'output.txt')
    assert os.path.exists(output_path)
    with open(output_path, 'rb') as f:
        contents = f.read()
        assert contents == WcJob.output_files[0][2]

def test_publish_all_output(fixture):
    fixture['store'].add_test_job('test_publish_all_output_1', 'wc', 'destaged')
    fixture['store'].add_test_job('test_publish_all_output_2', 'wc', 'destaged')

    output_dir_1 = os.path.join(fixture['output-dir'], 'test_publish_all_output_1')
    os.mkdir(output_dir_1)
    output_dir_2 = os.path.join(fixture['output-dir'], 'test_publish_all_output_2')
    os.mkdir(output_dir_2)

    fixture['local-files'].publish_all_jobs_output()

    output_path_1 = os.path.join(output_dir_1, 'output.txt')
    assert os.path.exists(output_path_1)
    with open(output_path_1, 'rb') as f:
        contents = f.read()
        assert contents == WcJob.output_files[0][2]

    output_path_2 = os.path.join(output_dir_2, 'output.txt')
    assert os.path.exists(output_path_1)
    with open(output_path_2, 'rb') as f:
        contents = f.read()
        assert contents == WcJob.output_files[0][2]
