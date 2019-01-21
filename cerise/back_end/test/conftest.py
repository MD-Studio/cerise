import copy
import json
from pathlib import Path

import cerulean
import pytest
import yaml

from cerise.back_end.test.mock_job import MockJob
from cerise.job_store.job_state import JobState
from cerise.test.fixture_jobs import (BrokenJob, FileArrayJob, HostnameJob,
                                      MissingInputJob, NoSuchStepJob, PassJob,
                                      SecondaryFilesJob, SlowJob, WcJob)


def workflow_to_json(yaml_string, test_steps_dir):
    dict_form = yaml.safe_load(str(yaml_string, 'utf-8'))
    if dict_form['class'] == 'Workflow':
        if 'steps' in dict_form:
            steps = dict_form['steps']
            if isinstance(steps, list):
                for step in steps:
                    if 'run' in step:
                        step['run'] = '{}/{}'.format(test_steps_dir,
                                                     step['run'])
            elif isinstance(steps, dict):
                for _, step in steps.items():
                    if 'run' in step:
                        step['run'] = '{}/{}'.format(test_steps_dir,
                                                     step['run'])
    return bytes(json.dumps(dict_form), 'utf-8')


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
        return cerulean.LocalFileSystem() / str(self._exchange_path)

    def get_store_location_client(self):
        return 'client://{}'.format(self._exchange_path)


@pytest.fixture
def mock_config(tmpdir):
    return MockConfig(tmpdir)


@pytest.fixture
def local_api_dir():
    return Path(__file__).parents[2] / 'test' / 'api'


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
    MissingInputJob, BrokenJob
])
def mock_store_submitted(request, mock_config):
    store = MockStore(mock_config)
    job_fixture = request.param

    exchange_dir = mock_config.get_store_location_service()
    exchange_input_dir = exchange_dir / 'input'
    exchange_input_dir.mkdir()
    exchange_job_input_dir = exchange_input_dir / 'test_job'
    exchange_job_input_dir.mkdir()

    wf_path = exchange_job_input_dir / 'test_workflow.cwl'
    wf_path.write_bytes(job_fixture.workflow)

    for input_file in job_fixture.local_input_files:
        input_path = exchange_job_input_dir / input_file.location
        if input_file.location in job_fixture.input_content:
            input_path.write_bytes(
                job_fixture.input_content[input_file.location])
        input_file.source = input_path

        for sec_file in input_file.secondary_files:
            sec_path = exchange_job_input_dir / sec_file.location
            if sec_file.location in job_fixture.input_content:
                sec_path.write_bytes(
                    job_fixture.input_content[sec_file.location])
            sec_file.source = sec_path

    job = MockJob(
        'test_job', 'test_job', 'client://' + str(wf_path),
        job_fixture.local_input('client://' + str(exchange_job_input_dir) +
                                '/'))
    job.state = JobState.SUBMITTED

    store.add_job(job)
    yield store, job_fixture

    for input_file in job_fixture.local_input_files:
        input_file.source = None

        for sec_file in input_file.secondary_files:
            sec_file.source = None


@pytest.fixture(params=[
    PassJob, HostnameJob, WcJob, SlowJob, SecondaryFilesJob, FileArrayJob,
    MissingInputJob, NoSuchStepJob
])
def mock_store_resolved(request, mock_config):
    store = MockStore(mock_config)
    job_fixture = copy.deepcopy(request.param)

    exchange_dir = mock_config.get_store_location_service()
    exchange_job_input_dir = exchange_dir / 'input' / 'test_job'
    exchange_job_input_dir.mkdir(parents=True)
    local_input = job_fixture.local_input('file://' +
                                          str(exchange_job_input_dir) + '/')

    for input_file in job_fixture.local_input_files:
        input_path = exchange_job_input_dir / input_file.location
        if input_file.location in job_fixture.input_content:
            input_path.write_bytes(
                job_fixture.input_content[input_file.location])
        input_file.source = input_path

        for sec_file in input_file.secondary_files:
            sec_path = exchange_job_input_dir / sec_file.location
            if sec_file.location in job_fixture.input_content:
                sec_path.write_bytes(
                    job_fixture.input_content[sec_file.location])
            sec_file.source = sec_path

    job = MockJob('test_job', 'test_job', None, local_input)
    job.workflow_content = job_fixture.workflow
    job.state = JobState.STAGING_IN

    store.add_job(job)
    yield store, job_fixture

    for input_file in job_fixture.local_input_files:
        input_file.source = None

        for sec_file in input_file.secondary_files:
            sec_file.source = None


@pytest.fixture(params=[
    PassJob, HostnameJob, WcJob, SlowJob, SecondaryFilesJob, FileArrayJob
])
def mock_store_staged(request, mock_config):
    store = MockStore(mock_config)
    job_fixture = request.param

    remote_base = mock_config.get_basedir()
    job_dir = remote_base / 'jobs' / 'test_job'
    work_dir = job_dir / 'work'
    work_dir.mkdir(parents=True)

    test_steps_dir = mock_config.get_basedir() / 'api' / 'test' / 'steps'

    (job_dir / 'workflow.cwl').write_bytes(
        workflow_to_json(job_fixture.workflow, test_steps_dir))
    (job_dir / 'input.json').write_text(
        json.dumps(job_fixture.remote_input(work_dir)))

    for _, name, content in job_fixture.remote_input_files:
        (work_dir / name).write_bytes(content)

    job = MockJob('test_job', 'test_job', None, None)
    job.workflow_content = job_fixture.workflow
    job.remote_workdir_path = str(work_dir)
    job.remote_workflow_path = str(job_dir / 'workflow.cwl')
    job.remote_input_path = str(job_dir / 'input.json')
    job.remote_stdout_path = str(job_dir / 'stdout.txt')
    job.remote_stderr_path = str(job_dir / 'stderr.txt')
    job.remote_system_out_path = str(job_dir / 'sysout.txt')
    job.remote_system_err_path = str(job_dir / 'syserr.txt')
    job.state = JobState.STAGING_IN

    store.add_job(job)
    return store, job_fixture


@pytest.fixture(params=[
    PassJob, HostnameJob, WcJob, SlowJob, SecondaryFilesJob, FileArrayJob
])
def mock_store_run(request, mock_config):
    store = MockStore(mock_config)
    job_fixture = request.param

    remote_base = mock_config.get_basedir()
    job_dir = remote_base / 'jobs' / 'test_job'
    work_dir = job_dir / 'work'
    work_dir.mkdir(parents=True)

    (job_dir / 'stdout.txt').write_text(
        job_fixture.remote_output('file://{}'.format(work_dir)))
    (job_dir / 'stderr.txt').write_text('Test log output\nAnother line\n')

    for location, content in job_fixture.output_content.items():
        (work_dir / location).write_bytes(content)

    job = MockJob('test_job', 'test_job', None, None)
    job.remote_workdir_path = str(work_dir)
    job.remote_stdout_path = str(job_dir / 'stdout.txt')
    job.remote_stderr_path = str(job_dir / 'stderr.txt')
    job.state = JobState.STAGING_IN

    store.add_job(job)
    return store, job_fixture


@pytest.fixture
def mock_store_run_and_updated(mock_config, mock_store_run):
    store, job_fixture = mock_store_run

    work_dir = mock_config.get_basedir() / 'jobs' / 'test_job' / 'work'

    for output_file in job_fixture.output_files:
        output_path = work_dir / output_file.location
        if output_file.location in job_fixture.output_content:
            output_path.write_bytes(
                job_fixture.output_content[output_file.location])
        output_file.source = output_path

    job = store.get_job('test_job')
    job.remote_output = job_fixture.remote_output('file://{}'.format(work_dir))

    return store, job_fixture


@pytest.fixture(params=[
    PassJob, HostnameJob, WcJob, SlowJob, SecondaryFilesJob, FileArrayJob
])
def mock_store_destaged(request, mock_config):
    store = MockStore(mock_config)
    job_fixture = request.param

    work_dir = mock_config.get_basedir() / 'jobs' / 'test_job' / 'work'
    work_dir.mkdir(parents=True)

    for output_file in job_fixture.output_files:
        output_path = work_dir / output_file.location
        if output_file.location in job_fixture.output_content:
            output_path.write_bytes(
                job_fixture.output_content[output_file.location])
        output_file.source = output_path

    job = MockJob('test_job', 'test_job', None, None)
    job.remote_output = job_fixture.remote_output('file://{}'.format(work_dir))

    store.add_job(job)

    yield store, job_fixture

    for output_file in job_fixture.output_files:
        output_file.source = None
