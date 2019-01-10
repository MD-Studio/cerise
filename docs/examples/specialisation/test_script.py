#!/usr/bin/env python3

import cerise_client.service as cs
import time


srv = cs.require_managed_service('cerise-example-test', 29593, 'cerise-example')

# If your specialisation is not working, the following will print the
# Cerise server log. That should give you an error message describing
# what went wrong.
# print(srv.get_log())

job = srv.create_job('test_job')
job.set_workflow('test_workflow.cwl')
job.run()

while job.is_running():
    time.sleep(0.1)

# If something goes wrong executing the job, this will print the job log.
# print(job.log)

print(job.outputs['output'].text)

srv.destroy_job(job)
cs.destroy_managed_service(srv)
