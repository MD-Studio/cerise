from cerise.back_end.remote_job_files import RemoteJobFiles


import pytest


def _remote_files_are_equal(output_file, reference_output_file, prefix):
    if output_file.name != reference_output_file.name:
        return False
    if output_file.index != reference_output_file.index:
        return False
    if output_file.location != (prefix + reference_output_file.location):
        return False
    if output_file.content != reference_output_file.content:
        return False
    for i, secondary_file in enumerate(output_file.secondary_files):
        return _remote_files_are_equal(secondary_file, reference_output_file.secondary_files[i], prefix)
    return True


def test_stage_job(mock_config, mock_store_resolved):
    store, job_fixture = mock_store_resolved

    remote_job_files = RemoteJobFiles(store, mock_config)

    input_files = job_fixture.local_input_files
    remote_job_files.stage_job('test_job', input_files, job_fixture.workflow)

    remote_base = mock_config.get_basedir()
    jobdir = remote_base / 'jobs' / 'test_job'

    workflow_file = jobdir / 'workflow.cwl'
    assert workflow_file.read_bytes() == job_fixture.workflow

    for _, path, content in job_fixture.remote_input_files:
        staged_file = jobdir / 'work' / path
        assert staged_file.read_bytes() == content


def test_update_job(mock_config, mock_store_run):
    store, job_fixture = mock_store_run

    remote_job_files = RemoteJobFiles(store, mock_config)
    work_dir = mock_config.get_basedir() / 'jobs' / 'test_job' / 'work'

    remote_job_files.update_job('test_job')
    job = store.get_job('test_job')
    assert job.remote_output == job_fixture.remote_output('file://{}'.format(work_dir))
    assert job.remote_error == 'Test log output\nAnother line\n'


def test_destage_job(mock_config, mock_store_run_and_updated):
    store, job_fixture = mock_store_run_and_updated

    remote_job_files = RemoteJobFiles(store, mock_config)
    output_files = remote_job_files.destage_job_output('test_job')
    for i, output_file in enumerate(output_files):
        assert _remote_files_are_equal(output_file, job_fixture.output_files[i], '')


def test_delete_job(mock_config, mock_store_run_and_updated):
    store, job_fixture = mock_store_run_and_updated

    remote_job_files = RemoteJobFiles(store, mock_config)

    job_dir = mock_config.get_basedir() / 'jobs' / 'test_job'
    assert job_dir.exists()
    remote_job_files.delete_job('test_job')
    assert not job_dir.exists()
