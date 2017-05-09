import webdav.client as wc
from bravado.client import SwaggerClient

import requests
import time

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

