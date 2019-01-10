import pytest

from cerise.back_end.local_files import LocalFiles
from cerise.test.fixture_jobs import BrokenJob


@pytest.fixture
def output_dir(mock_config):
    exchange_dir = mock_config.get_store_location_service()
    output_dir = exchange_dir / 'output' / 'test_job'
    return output_dir


def _assert_local_files_are_equal(input_file, reference_input_file, prefix):
    assert input_file.name == reference_input_file.name
    assert input_file.index == reference_input_file.index
    assert input_file.location == (prefix + reference_input_file.location)
    assert str(input_file.source) == str(reference_input_file.source)
    for i, secondary_file in enumerate(input_file.secondary_files):
        _assert_local_files_are_equal(
            secondary_file, reference_input_file.secondary_files[i], prefix)


def test_resolve_input(mock_config, mock_store_submitted):
    store, job_fixture = mock_store_submitted

    local_files = LocalFiles(store, mock_config)
    if job_fixture == BrokenJob:
        with pytest.raises(ValueError):
            local_files.resolve_input('test_job')
    else:
        input_files = local_files.resolve_input('test_job')

        assert store.get_job(
            'test_job').workflow_content == job_fixture.workflow

        for i, input_file in enumerate(input_files):
            _assert_local_files_are_equal(
                input_file, job_fixture.local_input_files[i],
                mock_config.get_store_location_client() + '/input/test_job/')


def test_create_output_dir(mock_config, mock_store_destaged, output_dir):
    store, job_fixture = mock_store_destaged

    local_files = LocalFiles(store, mock_config)

    assert not output_dir.exists()
    local_files.create_output_dir('test_job')
    assert output_dir.exists()


def test_delete_output_dir(mock_config, mock_store_destaged, output_dir):
    store, job_fixture = mock_store_destaged

    local_files = LocalFiles(store, mock_config)

    output_dir.mkdir()
    (output_dir / 'output.txt').write_text('Test output')

    local_files.delete_output_dir('test_job')
    assert not output_dir.exists()


def test_publish_output(mock_config, mock_store_destaged, output_dir):
    store, job_fixture = mock_store_destaged

    local_files = LocalFiles(store, mock_config)
    local_files.publish_job_output('test_job', job_fixture.output_files)

    for location, content in job_fixture.output_content.items():
        assert (output_dir / location).read_bytes() == content
