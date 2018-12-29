from cerise.back_end.local_files import LocalFiles
from .mock_store import MockStore

from cerise.back_end.test.fixture_jobs import PassJob
from cerise.back_end.test.fixture_jobs import WcJob
from cerise.back_end.test.fixture_jobs import SecondaryFilesJob

import os
import pytest


@pytest.fixture
def fixture(request, mock_config):
    result = {}

    result['local-files-config'] = mock_config

    # strip off the file:// prefix
    basedir = mock_config.get_store_location_service()[7:]

    result['output-dir'] = os.path.join(basedir, 'output')

    result['store'] = MockStore({
        'local-base-path': basedir
        })

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
    input_files = fixture['local-files'].resolve_input('test_resolve_input')
    assert fixture['store'].get_job('test_resolve_input').workflow_content == WcJob.workflow
    assert input_files[0].name == WcJob.local_input_files[0].name
    assert input_files[0].content == WcJob.local_input_files[0].content

def test_resolve_missing_input(fixture):
    fixture['store'].add_test_job('test_missing_input', 'missing_input', 'submitted')
    with pytest.raises(FileNotFoundError):
        fixture['local-files'].resolve_input('test_missing_input')

def test_resolve_secondary_files(fixture):
    fixture['store'].add_test_job('test_resolve_secondary_files', 'secondary_files', 'submitted')
    input_files = fixture['local-files'].resolve_input('test_resolve_secondary_files')
    assert fixture['store'].get_job('test_resolve_secondary_files').workflow_content == SecondaryFilesJob.workflow
    assert input_files[0].name == SecondaryFilesJob.local_input_files()[0].name
    assert input_files[0].content == SecondaryFilesJob.local_input_files()[0].content
    assert input_files[0].secondary_files[0].content == \
            SecondaryFilesJob.local_input_files()[0].secondary_files[0].content

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
    fixture['store'].add_test_job('test_publish_no_output', 'pass', 'destaged')
    output_dir = os.path.join(fixture['output-dir'], 'test_publish_no_output')
    os.mkdir(output_dir)
    fixture['local-files'].publish_job_output('test_publish_no_output', None)
    assert os.listdir(output_dir) == []

def test_publish_output(fixture):
    fixture['store'].add_test_job('test_publish_output', 'wc', 'destaged')
    output_files = fixture['store'].get_output_files('wc')

    fixture['local-files'].publish_job_output('test_publish_output', output_files)

    output_path = os.path.join(fixture['output-dir'], 'test_publish_output', 'output.txt')
    assert os.path.exists(output_path)
    with open(output_path, 'rb') as f:
        contents = f.read()
        assert contents == WcJob.output_files[0][2]
