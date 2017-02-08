# coding: utf-8

from __future__ import absolute_import

from swagger_server.models.job import Job
from swagger_server.models.job_description import JobDescription
from . import BaseTestCase
from six import BytesIO
from flask import json


class TestDefaultController(BaseTestCase):
    """ DefaultController integration test stubs """

    def _create_test_job(self):
        """
        Creates a job for the test cases to work with.
        """
        body = JobDescription(
                name='test_job',
                workflow='https://github.com/common-workflow-language/common-workflow-language/raw/master/v1.0/v1.0/wc-tool.cwl',
                input={
                        "file1": {
                            "class": "File",
                            "location": "whale.txt"
                            }
                        }
                )
        response = self.client.open('/jobs',
                                    method='POST',
                                    data=json.dumps(body),
                                    content_type='application/json')
        self.assert200(response, "Response body is : " + response.data.decode('utf-8'))
        return response.json

    def test_cancel_job_by_id(self):
        """
        Test case for cancel_job_by_id

        Cancel a job
        """
        test_job = self._create_test_job()
        # Cancel test job
        response = self.client.open('/jobs/{jobId}/cancel'.format(jobId=test_job['id']),
                                    method='POST')
        self.assert200(response, "Response body is : " + response.data.decode('utf-8'))

        # Check that state is now cancelled
        response = self.client.open('/jobs/{jobId}'.format(jobId=test_job['id']))
        job = response.json
        assert(job['state'] == "Cancelled")

    def test_delete_job_by_id(self):
        """
        Test case for delete_job_by_id

        Deleta a job
        """
        test_job = self._create_test_job()
        # Delete test job
        response = self.client.open('/jobs/{jobId}'.format(jobId=test_job['id']),
                                    method='DELETE')
        self.assertStatus(response, 204, "Response body is : " + response.data.decode('utf-8'))

        # Check that job has been deleted
        response = self.client.open('/jobs/{jobId}'.format(jobId=test_job['id']))
        self.assert404(response, "Response body is : " + response.data.decode('utf-8'))

    def test_get_job_by_id(self):
        """
        Test case for get_job_by_id

        Get a job
        """
        test_job = self._create_test_job()
        # Get the job
        response = self.client.open('/jobs/{jobId}'.format(jobId=test_job['id']),
                                    method='GET')
        self.assert200(response, "Response body is : " + response.data.decode('utf-8'))
        assert all(item in response.json for item in test_job)

    def test_get_job_log_by_id(self):
        """
        Test case for get_job_log_by_id

        Log of a job
        """
        test_job = self._create_test_job()
        # Get the log
        response = self.client.open('/jobs/{jobId}/log'.format(jobId=test_job['id']),
                                    method='GET')
        self.assert200(response, "Response body is : " + response.data.decode('utf-8'))

    def test_get_jobs(self):
        """
        Test case for get_jobs

        list of jobs
        """
        response = self.client.open('/jobs',
                                    method='GET')
        self.assert200(response, "Response body is : " + response.data.decode('utf-8'))

    def test_post_job(self):
        """
        Test case for post_job

        submit a new job
        """
        test_job = self._create_test_job()
        assert test_job['state'] == 'Waiting'

if __name__ == '__main__':
    import unittest
    unittest.main()
