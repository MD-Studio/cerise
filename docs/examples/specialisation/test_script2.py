#!/usr/bin/env python3

import cerise_client.service as cs
import time


srv = cs.require_managed_service('cerise-example-test', 29593, 'cerise-example')
job1 = srv.create_job('test_job1')
job1.set_workflow('test_workflow2.cwl')
job1.run()

job2 = srv.create_job('test_job2')
job2.set_workflow('test_workflow3.cwl')
job2.add_input_file('input_file', 'message.txt')
job2.run()

while job1.is_running() or job2.is_running():
    time.sleep(0.1)

print(job1.outputs['output'].text)
print(job2.outputs['output'].text)

srv.destroy_job(job1)
srv.destroy_job(job2)
cs.destroy_managed_service(srv)
