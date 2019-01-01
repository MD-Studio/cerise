from bravado.client import SwaggerClient
from bravado.exception import HTTPBadGateway, HTTPNotFound
from bravado_core.formatter import SwaggerFormat
import docker
import io
import json
from pathlib import Path
import pytest
import tarfile
import time
import webdav.client as wc
import webdav.urn as wu


from cerise.test.fixture_jobs import (
        PassJob, HostnameJob, WcJob, SlowJob, SecondaryFilesJob, FileArrayJob,
        MissingInputJob, BrokenJob, NoWorkflowJob)


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
def cerise_service():
    client = docker.from_env()

    clear_old_container(client, 'cerise-test-service')
    clear_old_container(client, 'cerise-test-slurm')

    client.images.pull('mdstudio/cerulean-test-slurm-18-08:latest')
    slurm_image = client.images.get(
            'mdstudio/cerulean-test-slurm-18-08:latest')
    slurm_container = client.containers.run(
            slurm_image, name='cerise-test-slurm', detach=True)

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

    yield

    service_container.stop()

    cur_dir = Path(__file__).parent
    try:
        stream, _ = service_container.get_archive('/home/cerise/.coverage')
        buf = io.BytesIO(stream.read())
        coverage_file = cur_dir.parents[1] / '.coverage.integration_test'
        with tarfile.open(fileobj=buf) as archive:
            with archive.extractfile('.coverage') as cov_data:
                with coverage_file.open('wb') as cov_file:
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


@pytest.fixture(params=[MissingInputJob, BrokenJob, NoWorkflowJob])
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


def _start_job(cerise_client, webdav_client, job_fixture):
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
        input_res.read_from(io.BytesIO(input_file.content))

        for secondary_file in input_file.secondary_files:
            input_path = '{}/{}'.format(input_dir, secondary_file.location)
            input_res = wc.Resource(webdav_client, wu.Urn(input_path))
            input_res.read_from(io.BytesIO(secondary_file.content))

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

    print(str(response))
    assert response.status_code == 201
    return job


def test_get_jobs(cerise_service, cerise_client):
    (jobs, response) = cerise_client.jobs.get_jobs().result()
    assert response.status_code == 200


def test_run_job(cerise_service, cerise_client, webdav_client,
                 job_fixture_success):
    job = _start_job(cerise_client, webdav_client, job_fixture_success)
    assert job.state == 'Waiting'

    while job.state == 'Waiting' or job.state == 'Running':
        job, response = cerise_client.jobs.get_job_by_id(jobId=job.id).result()

    assert job.state == 'Success'


def test_run_broken_job(cerise_service, cerise_client, webdav_client,
                        job_fixture_permfail, debug_output):

    job = _start_job(cerise_client, webdav_client, job_fixture_permfail)
    assert job.state == 'Waiting'

    while job.state == 'Waiting' or job.state == 'Running':
        job, response = cerise_client.jobs.get_job_by_id(jobId=job.id).result()

    assert job.state == 'PermanentFailure'


def test_get_job_by_id(cerise_service, cerise_client, webdav_client):
    job1 = _start_job(cerise_client, webdav_client, WcJob)
    job2 = _start_job(cerise_client, webdav_client, SecondaryFilesJob)

    job, response = cerise_client.jobs.get_job_by_id(jobId=job1.id).result()
    assert job.name == job1.name
    assert job.id == job1.id

    job, response = cerise_client.jobs.get_job_by_id(jobId=job2.id).result()
    assert job.name == job2.name
    assert job.id == job2.id
