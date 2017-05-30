import webdav.client as wc
from bravado.client import SwaggerClient
from bravado.exception import HTTPNotFound

import docker
import json
import os
import pytest
import requests
import time

@pytest.fixture(scope="module")
def docker_image(request):
    client = docker.from_env()
    try:
        old_container = client.containers.get('simple-cwl-xenon-service-integration-test-container')
        old_container.stop()
        old_container.remove()
    except docker.errors.NotFound:
        pass

    image = client.images.build(
            path='.',
            rm=True,
            tag='simple-cwl-xenon-service-integration-test-image')

    return image.id

@pytest.fixture()
def docker_client(request):
    return docker.from_env()

@pytest.fixture
def service(request, tmpdir, docker_client, docker_image):
    scxs_container = docker_client.containers.run(
            docker_image,
            name='simple-cwl-xenon-service-integration-test-container',
            ports={ '29593/tcp': '29593', '29594/tcp': '29594'},
            detach=True)
    time.sleep(3)   # Give it some time to start up
    yield True

    # Tear down
    scxs_container.stop()

    # Collect logs for debugging
    archive_file = os.path.join(str(tmpdir), 'docker_logs.tar')
    stream, stat = scxs_container.get_archive('/var/log')
    with open(archive_file, 'wb') as f:
        f.write(stream.read())

    scxs_container.remove()

@pytest.fixture
def webdav_client(request, service):
    return wc.Client({'webdav_hostname': 'http://localhost:29594'})

@pytest.fixture
def service_client(request, service):
    bravado_config = {
        'validate_responses': False,
        'also_return_response': True
        }
    return SwaggerClient.from_url('http://localhost:29593/swagger.json', config=bravado_config)

def test_get_jobs(service_client):
    print(service_client.jobs.get_jobs().result())
    pass

def _create_test_job(name, cwlfile, inputfile, webdav, service):
    """
    Creates a job for the test cases to work with.

    Args:
        name (str): Name of the test job
        cwlfile (str): Name of the CWL file to use (in current dir)
        inputfile (str): Name of input file to use
        webdav (wc.Client): WebDAV client fixture
        service (SwaggerClient): REST client fixture
    """
    cur_dir = os.path.dirname(__file__)
    test_workflow = os.path.join(cur_dir, cwlfile)
    webdav.mkdir('/input/' + name)
    remote_workflow_path = '/input/' + name + '/' + cwlfile
    webdav.upload_sync(local_path = test_workflow, remote_path = remote_workflow_path)

    test_input = os.path.join(cur_dir, inputfile)
    with open(test_input, 'r') as f:
        input_data = json.load(f)

    JobDescription = service.get_model('job-description')
    job_desc = JobDescription(
        name=name,
        workflow='http://localhost:29594' + remote_workflow_path,
        input=input_data
    )
    (job, response) = service.jobs.post_job(body=job_desc).result()
    print(str(response))
    assert response.status_code == 201
    return job

def test_cancel_job_by_id(service, webdav_client, service_client):
    """
    Test case for cancel_job_by_id

    Cancel a job
    """
    test_job = _create_test_job('test_cancel_job_by_id',
            'slow_job.cwl', 'null_input.json',
            webdav_client, service_client)

    # Cancel test job
    (out, response) = service_client.jobs.cancel_job_by_id(jobId=test_job.id).result()

    # Check that state is now cancelled
    (updated_job, response) = service_client.jobs.get_job_by_id(jobId=test_job.id).result()
    assert response.status_code == 200
    assert updated_job.state == 'Cancelled'

def test_delete_job_by_id(service, webdav_client, service_client):
    """
    Test case for delete_job_by_id

    Delete a job.
    """
    test_job = _create_test_job('test_delete_job_by_id',
            'test_workflow.cwl', 'test_input.json',
            webdav_client, service_client)

    service_client.jobs.delete_job_by_id(jobId=test_job.id).result()

    with pytest.raises(HTTPNotFound):
        (updated_job, response) = service_client.jobs.get_job_by_id(jobId=test_job.id).result()


def test_get_job_by_id(service, webdav_client, service_client):
    """
    Test case for get_job_by_id

    Get a job
    """
    test_job = _create_test_job('test_get_job_by_id',
            'test_workflow.cwl', 'test_input.json',
            webdav_client, service_client)

    time.sleep(0.5)
    (job, response) = service_client.jobs.get_job_by_id(jobId=test_job.id).result()

    print(job)
    assert job.name == test_job.name
    assert job.workflow == test_job.workflow
    assert job.input == test_job.input
    assert job.state == 'Success'

    out_file_location = job.output['output']['location']
    print(out_file_location)
    out_data = requests.get(out_file_location)
    assert out_data.status_code == 200
    assert out_data.text == 'Hello world!\n'


def test_get_job_log_by_id(service, webdav_client, service_client):
    """
    Test case for get_job_log_by_id

    Log of a job
    """
    test_job = _create_test_job('test_get_job_log_by_id',
            'test_workflow.cwl', 'test_input.json',
            webdav_client, service_client)

#   Disable the following for now, Bravado only supports JSON responses, and
#   this is plain text :(

#   time.sleep(1)
#   (log, response) = service_client.jobs.get_job_log_by_id(jobId=test_job.id).result()
#   assert response.status_code == 200




'''
webdav.mkdir('/input/testjob')
webdav.upload_sync(local_path = 'test_workflow.cwl', remote_path = '/input/testjob/test_workflow.cwl')
#webdav.upload_sync(local_path = 'hello_world.txt', remote_path = '/testjob/hello_world.txt')

bravado_config = {
    'validate_responses': False
    }
jobrunner = SwaggerClient.from_url('http://localhost:29593/swagger.json', config=bravado_config)

JobDescription = jobrunner.get_model('job-description')

job_desc = JobDescription(
        name='integration_test_job',
        workflow='http://localhost:29594/input/testjob/test_workflow.cwl',
        input={'message': 'Hello, World!'})

job = jobrunner.jobs.post_job(body=job_desc).result()

print("\nJob:")
print(job)

job_list = jobrunner.jobs.get_jobs().result()

# print("List of jobs")
# print(job_list)

time.sleep(1)

job = jobrunner.jobs.get_job_by_id(jobId=job.id).result()
print("\nUpdated job:")
print(job)

print("\nOutput:")
print(job['output'].output)

output_file = requests.get(job['output'].output['location']).text
print("\n")
print(output_file)
'''

