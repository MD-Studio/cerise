import webdav.client as wc
from bravado.client import SwaggerClient

import docker
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
def dockers(request, docker_client, docker_image):
    scxs_container = docker_client.containers.run(
            docker_image,
            name='simple-cwl-xenon-service-integration-test-container',
            ports={ '29593/tcp': '29593', '29594/tcp': '29594'},
            detach=True)
    time.sleep(3)   # Give it some time to start up
    yield True
    scxs_container.stop()
    scxs_container.remove()

@pytest.fixture
def webdav_client(request, dockers):
    return wc.Client({'webdav_hostname': 'http://localhost:29594'})

@pytest.fixture
def service_client(request, dockers):
    bravado_config = { 'validate_responses': False }
    return SwaggerClient.from_url('http://localhost:29593/swagger.json', config=bravado_config)

def test_get_jobs(service_client):
    print(service_client.jobs.get_jobs().result())
    pass

'''
webdav = wc.Client({'webdav_hostname': 'http://localhost:29594'})
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

