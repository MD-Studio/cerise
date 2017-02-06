# coding: utf-8

from __future__ import absolute_import

from swagger_server.models.job import Job
from swagger_server.models.job_description import JobDescription
from . import BaseTestCase
from six import BytesIO
from flask import json


class TestDefaultController(BaseTestCase):
    """ DefaultController integration test stubs """

    def test_cancel_job_by_id(self):
        """
        Test case for cancel_job_by_id

        Cancel a job
        """
        response = self.client.open('/jobs/{jobId}/cancel'.format(jobId='jobId_example'),
                                    method='POST')
        self.assert200(response, "Response body is : " + response.data.decode('utf-8'))

    def test_delete_job_by_id(self):
        """
        Test case for delete_job_by_id

        Deleta a job
        """
        response = self.client.open('/jobs/{jobId}'.format(jobId='jobId_example'),
                                    method='DELETE')
        self.assert200(response, "Response body is : " + response.data.decode('utf-8'))

    def test_get_job_by_id(self):
        """
        Test case for get_job_by_id

        Get a job
        """
        response = self.client.open('/jobs/{jobId}'.format(jobId='jobId_example'),
                                    method='GET')
        self.assert200(response, "Response body is : " + response.data.decode('utf-8'))

    def test_get_job_log_by_id(self):
        """
        Test case for get_job_log_by_id

        Log of a job
        """
        response = self.client.open('/jobs/{jobId}/log'.format(jobId='jobId_example'),
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
        body = JobDescription()
        response = self.client.open('/jobs',
                                    method='POST',
                                    data=json.dumps(body),
                                    content_type='application/json')
        self.assert200(response, "Response body is : " + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
