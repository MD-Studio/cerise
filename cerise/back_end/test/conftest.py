from pathlib import Path

import cerulean
import pytest

from cerise.job_store.in_memory_job import InMemoryJob
from cerise.back_end.test.fixture_jobs import (PassJob, HostnameJob, WcJob,
        SlowJob, SecondaryFilesJob, FileArrayJob, MissingInputJob)


class MockConfig:
    def __init__(self, tmpdir):
        self._file_system = cerulean.LocalFileSystem()
        base_dir_path = tmpdir / 'remote_basedir'
        base_dir_path.mkdir()
        self._base_dir = self._file_system / str(base_dir_path)
        exchange_path = tmpdir / 'local_exchange'
        exchange_path.mkdir()
        self._exchange_path = str(exchange_path)

    def get_scheduler(self, run_on_head_node=False):
        term = cerulean.LocalTerminal()
        return cerulean.DirectGnuScheduler(term)

    def get_queue_name(self):
        return None

    def get_slots_per_node(self):
        return 1

    def get_cores_per_node(self):
        return 16

    def get_scheduler_options(self):
        return None

    def get_remote_cwl_runner(self):
        return '$CERISE_API/cerise/files/cwltiny.py'

    def get_file_system(self):
        return self._file_system

    def get_basedir(self):
        return self._base_dir

    def get_username(self, kind):
        return None

    def get_store_location_service(self):
        return 'file://{}/'.format(self._exchange_path)

    def get_store_location_client(self):
        return 'client://{}'.format(self._exchange_path)


@pytest.fixture
def mock_config(tmpdir):
    return MockConfig(tmpdir)


class MockStore:
    def __init__(self, config):
        self._config = config
        self._jobs = []
        self.deleted_jobs = []

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def add_job(self, job):
        """Not part of interface, for testing."""
        self._jobs.append(job)

    def list_jobs(self):
        return self._jobs

    def get_job(self, job_id):
        return [job for job in self._jobs if job.id == job_id][0]

    def delete_job(self, job_id):
        self.deleted_jobs.extend(
                [job for job in self._jobs if job.id == job_id])


@pytest.fixture(params=[
        PassJob, HostnameJob, WcJob, SlowJob, SecondaryFilesJob, FileArrayJob,
        MissingInputJob])
def mock_store_submitted(request, mock_config):
    store = MockStore(mock_config)
    job_fixture = request.param

    exchange_dir = Path(mock_config.get_store_location_service()[7:])
    exchange_input_dir = exchange_dir / 'input'
    exchange_input_dir.mkdir()
    exchange_job_input_dir = exchange_input_dir / 'test_job'
    exchange_job_input_dir.mkdir()

    wf_path = exchange_job_input_dir / 'test_workflow.cwl'

    with wf_path.open('wb') as f:
        f.write(job_fixture.workflow)

    for input_file in job_fixture.local_input_files:
        input_path = exchange_dir / input_file.location
        with input_path.open('wb') as f:
            f.write(input_file.content)

        for secondary_file in input_file.secondary_files:
            sf_path = exchange_dir / secondary_file.location
            with sf_path.open('wb') as f:
                f.write(secondary_file.content)


    job = InMemoryJob('test_job', 'test_job', 'client://' + str(wf_path),
                      job_fixture.local_input('file://' + str(exchange_dir) + '/'))

    store._jobs = [job]

    return store, job_fixture
