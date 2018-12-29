from cerise.back_end.local_files import LocalFiles
from .mock_store import MockStore

from cerise.back_end.test.fixture_jobs import WcJob
from cerise.back_end.test.fixture_jobs import MissingInputJob

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


def _local_files_are_equal(input_file, reference_input_file, prefix):
    if input_file.name != reference_input_file.name:
        return False
    if input_file.index != reference_input_file.index:
        return False
    if input_file.location != (prefix + reference_input_file.location):
        return False
    if input_file.content != reference_input_file.content:
        return False
    for i, secondary_file in enumerate(input_file.secondary_files):
        return _local_files_are_equal(secondary_file, reference_input_file.secondary_files[i], prefix)
    return True


def test_resolve_input(mock_config, mock_store_submitted):
    store, job_fixture = mock_store_submitted

    local_files = LocalFiles(store, mock_config)
    if job_fixture == MissingInputJob:
        with pytest.raises(FileNotFoundError):
            local_files.resolve_input('test_job')
    else:
        input_files = local_files.resolve_input('test_job')

        assert store.get_job('test_job').workflow_content == job_fixture.workflow

        for i, input_file in enumerate(input_files):
            assert _local_files_are_equal(
                    input_file, job_fixture.local_input_files[i],
                    mock_config.get_store_location_service())


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
