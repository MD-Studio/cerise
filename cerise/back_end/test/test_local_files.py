from cerise.back_end.local_files import LocalFiles
from cerise.test.fixture_jobs import MissingInputJob

from pathlib import Path

import pytest


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


def test_create_output_dir(mock_config, mock_store_destaged):
    store, job_fixture = mock_store_destaged

    local_files = LocalFiles(store, mock_config)

    exchange_dir = Path(mock_config.get_store_location_service()[7:])
    output_dir = exchange_dir / 'output' / 'test_job'

    assert not output_dir.exists()
    local_files.create_output_dir('test_job')
    assert output_dir.exists()


def test_delete_output_dir(mock_config, mock_store_destaged):
    store, job_fixture = mock_store_destaged

    local_files = LocalFiles(store, mock_config)

    exchange_dir = Path(mock_config.get_store_location_service()[7:])
    output_dir = exchange_dir / 'output' / 'test_job'

    output_dir.mkdir()
    (output_dir / 'output.txt').write_text('Test output')

    local_files.delete_output_dir('test_job')
    assert not output_dir.exists()


def test_publish_output(mock_config, mock_store_destaged):
    store, job_fixture = mock_store_destaged

    local_files = LocalFiles(store, mock_config)

    exchange_dir = Path(mock_config.get_store_location_service()[7:])
    output_dir = exchange_dir / 'output' / 'test_job'

    local_files.publish_job_output('test_job', job_fixture.output_files)

    for _, name, content in job_fixture.output_files:
        assert (output_dir / name).read_bytes() == content
