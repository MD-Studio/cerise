import bravado
from bravado.client import SwaggerClient
from bravado.exception import HTTPBadGateway, HTTPNotFound
from bravado_core.formatter import SwaggerFormat
import docker
import io
import json
from pathlib import Path
import pytest
import requests
import tarfile
import time
import webdav.client as wc
import webdav.urn as wu


from cerise.test.fixture_jobs import (
        PassJob, HostnameJob, WcJob, SlowJob, SecondaryFilesJob, FileArrayJob,
        LongRunningJob, NoSuchStepJob, MissingInputJob, BrokenJob,
        NoWorkflowJob, InstallScriptTestJob, PartiallyFailingJob)


def clear_old_container(client, name):
    # Remove any stale containers so that we can rebuild the image
    try:
        old_container = client.containers.get(name)
        old_container.stop()
        old_container.remove()
    except docker.errors.NotFound:
        pass


def wait_for_container(client, container):
    while container.status == 'created':
        time.sleep(0.1)
        container.reload()


@pytest.fixture(scope='session')
def clean_up_old_containers():
    client = docker.from_env()

    clear_old_container(client, 'cerise-test-service')
    clear_old_container(client, 'cerise-test-slurm')


@pytest.fixture(scope='session')
def slurm_container(clean_up_old_containers):
    client = docker.from_env()

    client.images.pull('mdstudio/cerulean-test-slurm-18-08:latest')
    slurm_image = client.images.get(
            'mdstudio/cerulean-test-slurm-18-08:latest')
    slurm_container = client.containers.run(
            slurm_image, name='cerise-test-slurm', hostname='hostname',
            detach=True)
    return slurm_container


@pytest.fixture(scope='session')
def cerise_service(slurm_container):
    client = docker.from_env()

    client.images.build(path='.', tag='cerise')
    service_image = client.images.build(path='cerise/test',
                                        tag='cerise-test-service')

    wait_for_container(client, slurm_container)
    service_container = client.containers.run(
            service_image, name='cerise-test-service',
            links={'cerise-test-slurm': 'cerise-test-slurm'},
            ports={'29593/tcp': ('127.0.0.1', 29593)},
            detach=True)
    wait_for_container(client, service_container)

    yield service_container

    service_container.stop()

    cur_dir = Path(__file__).parent
    try:
        stream, _ = service_container.get_archive('/home/cerise')
        buf = io.BytesIO(stream.read())
        with tarfile.open(fileobj=buf) as archive:
            for name in archive.getnames():
                if name.startswith('cerise/.coverage.'):
                    with archive.extractfile(name) as cov_data:
                        external_path = cur_dir.parents[1] / name[7:]
                        with external_path.open('wb') as cov_file:
                            cov_file.write(cov_data.read())
    except docker.errors.NotFound:
        print('Error: No coverage data found inside service container')
        pass

    service_container.remove()
    slurm_container.stop()
    slurm_container.remove()


@pytest.fixture
def webdav_client():
    return wc.Client({'webdav_hostname': 'http://localhost:29593'})


@pytest.fixture
def cerise_client():
    # Disable Bravado warning about uri format not being registered
    # It's all done by frameworks, so we're not testing that here
    uri_format = SwaggerFormat(
            description='A Uniform Resource Identifier',
            format='uri',
            to_wire=lambda uri: uri,
            to_python=lambda uri: uri,
            validate=lambda uri_string: True
    )

    bravado_config = {
        'also_return_response': True,
        'formats': [uri_format]
        }

    service = None
    start_time = time.perf_counter()
    cur_time = start_time
    while cur_time < start_time + 10:
        try:
            service = SwaggerClient.from_url(
                    'http://localhost:29593/swagger.json',
                    config=bravado_config)
            _, response = service.jobs.get_jobs().result()
            if response.status_code == 200:
                break
        except HTTPBadGateway:
            pass
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(0.1)
        cur_time = time.perf_counter()

    if cur_time >= start_time + 10:
        print("Warning: Cerise container failed to come up")
    return service


@pytest.fixture(params=[
        HostnameJob, WcJob, SecondaryFilesJob, FileArrayJob])
def job_fixture_success(request):
    return request.param


@pytest.fixture(params=[MissingInputJob, BrokenJob, NoWorkflowJob,
                        NoSuchStepJob, PartiallyFailingJob])
def job_fixture_permfail(request):
    return request.param


@pytest.fixture
def debug_output(request, tmpdir, cerise_client):
    yield

    client = docker.from_env()
    service = client.containers.get('cerise-test-service')
    slurm = client.containers.get('cerise-test-slurm')

    # Collect logs for debugging
    archive_file = tmpdir / 'docker_logs.tar'
    stream, _ = service.get_archive('/var/log')
    with archive_file.open('wb') as f:
        f.write(stream.read())

    # Collect run dir for debugging
    archive_file = tmpdir / 'docker_run.tar'
    stream, _ = service.get_archive('/home/cerise/run')
    with archive_file.open('wb') as f:
        f.write(stream.read())

    # Collect jobs dir for debugging
    archive_file = tmpdir / 'docker_jobs.tar'
    stream, _ = slurm.get_archive('/home/cerulean/.cerise/jobs')
    with archive_file.open('wb') as f:
        f.write(stream.read())

    # Collect API dir for debugging
    archive_file = tmpdir / 'docker_api.tar'
    stream, _ = slurm.get_archive('/home/cerulean/.cerise/api')
    with archive_file.open('wb') as f:
        f.write(stream.read())


def _start_job(cerise_client, webdav_client, job_fixture, test_name=None):
    if test_name is None:
        test_name = 'test_post_' + job_fixture.__name__

    input_dir = '/files/input/{}'.format(test_name)
    webdav_client.mkdir(input_dir)

    if job_fixture.workflow is not None:
        workflow_file = input_dir + '/workflow.cwl'
        workflow_res = wc.Resource(webdav_client, wu.Urn(workflow_file))
        workflow_res.read_from(io.BytesIO(job_fixture.workflow))
    else:
        workflow_file = ''

    for input_file in job_fixture.local_input_files:
        input_path = '{}/{}'.format(input_dir, input_file.location)
        input_res = wc.Resource(webdav_client, wu.Urn(input_path))
        if input_file.location in job_fixture.input_content:
            input_res.read_from(io.BytesIO(
                    job_fixture.input_content[input_file.location]))

        for secondary_file in input_file.secondary_files:
            input_path = '{}/{}'.format(input_dir, secondary_file.location)
            input_res = wc.Resource(webdav_client, wu.Urn(input_path))
            if secondary_file.location in job_fixture.input_content:
                input_res.read_from(io.BytesIO(
                    job_fixture.input_content[secondary_file.location]))

    input_dir_url = 'http://localhost:29593{}/'.format(input_dir)
    input_text = job_fixture.local_input(input_dir_url)
    input_data = json.loads(input_text)

    JobDescription = cerise_client.get_model('job-description')
    job_desc = JobDescription(
        name=test_name,
        workflow='http://localhost:29593' + workflow_file,
        input=input_data
    )

    job, response = cerise_client.jobs.post_job(body=job_desc).result()

    assert response.status_code == 201
    return job


def _wait_for_state(job_id, timeout, states, cerise_client):
    if states == 'DONE':
        states = ['Success', 'Cancelled', 'PermanentFailure',
                  'TemporaryFailure', 'SystemError']

    if isinstance(states, str):
        states = [states]

    def get_state(job_id):
        """Returns job/connection state.

        Returns a tuple of whether we could connect, if so whether the job
        exists, and if so the state.
        """
        try:
            test_job, response = cerise_client.jobs.get_job_by_id(
                    jobId=job_id).result()
            assert response.status_code == 200
        except HTTPNotFound:
            return True, False, None
        except requests.exceptions.ConnectionError:
            return False, None, None
        except HTTPBadGateway:
            return False, None, None
        return True, True, test_job

    connected, exists, test_job = get_state(job_id)

    start_time = time.perf_counter()
    while ((not connected or (exists and test_job.state not in states))
           and time.perf_counter() < start_time + timeout):
        time.sleep(0.1)
        connected, exists, test_job = get_state(job_id)

    assert time.perf_counter() < start_time + timeout
    assert ('DELETED' in states and not exists) or test_job.state in states
    return test_job


def _drop_connections(slurm_container):
    # this will drop all SSH connections for user cerulean from the server side
    # it doesn't kill sshd completely, because the main process runs as root
    slurm_container.exec_run('/bin/bash -c "killall sshd"', user='cerulean')


def test_get_jobs(cerise_service, cerise_client):
    _, response = cerise_client.jobs.get_jobs().result()
    assert response.status_code == 200


def test_api_install_script(cerise_service, cerise_client, webdav_client):
    job = _start_job(cerise_client, webdav_client, InstallScriptTestJob)
    job = _wait_for_state(job.id, 5.0, 'DONE', cerise_client)
    assert job.state == 'Success'

    output_path = '/files/output/{}/output.txt'.format(job.id)
    resource = wc.Resource(webdav_client, wu.Urn(output_path))
    output_buffer = io.BytesIO()
    resource.write_to(output_buffer)
    assert output_buffer.getvalue() == b'Testing API installation\n'


def test_run_job(cerise_service, cerise_client, webdav_client,
                 job_fixture_success):
    job = _start_job(cerise_client, webdav_client, job_fixture_success)
    assert job.state == 'Waiting'

    job = _wait_for_state(job.id, 5.0, 'DONE', cerise_client)
    assert job.state == 'Success'

    log, response = cerise_client.jobs.get_job_log_by_id(jobId=job.id).result()
    assert 'CWLTiny' in log
    assert 'success' in log

    # check that the URL given in the job description points to the log
    log_response = requests.get(job.log)
    assert log_response.status_code == 200
    assert log_response.text == log


def test_run_broken_job(cerise_service, cerise_client, webdav_client,
                        job_fixture_permfail):

    job = _start_job(cerise_client, webdav_client, job_fixture_permfail)
    assert job.state == 'Waiting'

    job = _wait_for_state(job.id, 5.0, 'DONE', cerise_client)
    assert job.state == 'PermanentFailure'

    if job_fixture_permfail == PartiallyFailingJob:
        assert ('missing_output' not in job.output or
                job.output['missing_output'] is None)

        out_data = requests.get(job.output['output']['location'])
        assert out_data.status_code == 200
        assert out_data.text == 'Running on host: hostname\n'


def test_get_job_by_id(cerise_service, cerise_client, webdav_client):
    job1 = _start_job(cerise_client, webdav_client, WcJob)
    job2 = _start_job(cerise_client, webdav_client, SecondaryFilesJob)

    job, response = cerise_client.jobs.get_job_by_id(jobId=job1.id).result()
    assert job.name == job1.name
    assert job.id == job1.id

    job, response = cerise_client.jobs.get_job_by_id(jobId=job2.id).result()
    assert job.name == job2.name
    assert job.id == job2.id


def test_cancel_waiting_job(cerise_service, cerise_client, webdav_client):
    start_time = time.perf_counter()
    job = _start_job(cerise_client, webdav_client, LongRunningJob,
                     'test_cancel_waiting_job')

    assert job.state == 'Waiting'
    _, response = cerise_client.jobs.cancel_job_by_id(jobId=job.id).result()
    assert response.status_code == 200

    job = _wait_for_state(job.id, 10.0, 'DONE', cerise_client)
    assert job.state == 'Cancelled'
    assert time.perf_counter() < start_time + 10.0


def test_cancel_running_job(cerise_service, cerise_client, webdav_client):
    start_time = time.perf_counter()
    job = _start_job(cerise_client, webdav_client, LongRunningJob,
                     'test_cancel_running_job')

    job = _wait_for_state(job.id, 10.0, 'Running', cerise_client)
    _, response = cerise_client.jobs.cancel_job_by_id(jobId=job.id).result()
    assert response.status_code == 200

    job = _wait_for_state(job.id, 10.0, 'DONE', cerise_client)
    assert job.state == 'Cancelled'
    assert time.perf_counter() < start_time + 10.0


def test_delete_job(cerise_service, cerise_client, webdav_client):
    job = _start_job(cerise_client, webdav_client, WcJob, 'test_delete_job')

    job = _wait_for_state(job.id, 5.0, 'Success', cerise_client)
    _, response = cerise_client.jobs.delete_job_by_id(jobId=job.id).result()
    assert response.status_code == 204

    _wait_for_state(job.id, 5.0, 'DELETED', cerise_client)


def test_delete_running_job(cerise_service, cerise_client, webdav_client):
    start_time = time.perf_counter()
    job = _start_job(cerise_client, webdav_client, LongRunningJob,
                     'test_delete_running_job')

    job = _wait_for_state(job.id, 5.0, 'Running', cerise_client)
    _, response = cerise_client.jobs.delete_job_by_id(jobId=job.id).result()
    assert response.status_code == 204

    _wait_for_state(job.id, 5.0, 'DELETED', cerise_client)


def test_restart_service(cerise_service, cerise_client, webdav_client,
                         slurm_container):
    job = _start_job(cerise_client, webdav_client, SlowJob,
                     'test_restart_service')
    job = _wait_for_state(job.id, 10.0, 'Running', cerise_client)
    cerise_service.stop()
    time.sleep(1)
    cerise_service.start()
    job = _wait_for_state(job.id, 5.0, 'DONE', cerise_client)
    assert job.state == 'Success'

    # the back-end is still re-installing the API here (dev mode)
    # so test that it'll survive a dropped connection while we're at it
    _drop_connections(slurm_container)

    # give it a bit to finish installing, so that it doesn't cause timeouts on
    # subsequent tests
    time.sleep(5)


def test_dropped_ssh_connection(cerise_service, cerise_client, webdav_client,
                            slurm_container):
    job = _start_job(cerise_client, webdav_client, SlowJob,
                     'test_dropped_ssh_connection')
    _drop_connections(slurm_container)

    job = _wait_for_state(job.id, 10.0, 'DONE', cerise_client)
    assert job.state == 'Success'


def test_no_resource_connection(cerise_service, cerise_client, webdav_client,
                       slurm_container):
    slurm_container.stop()
    time.sleep(1)
    job = _start_job(cerise_client, webdav_client, SlowJob,
                     'test_no_resource_connection')
    time.sleep(1)
    job, response = cerise_client.jobs.get_job_by_id(jobId=job.id).result()
    assert response.status_code == 200
    assert job.state == 'Waiting'

    slurm_container.start()
    job = _wait_for_state(job.id, 5.0, 'Running', cerise_client)

    job, response = cerise_client.jobs.get_job_by_id(jobId=job.id).result()
    assert response.status_code == 200
    assert job.state == 'Running'

    _drop_connections(slurm_container)

    job = _wait_for_state(job.id, 15.0, 'DONE', cerise_client)
    assert job.state == 'Success'
