import connexion
from swagger_server.models.job import Job
from swagger_server.models.job_description import JobDescription
from datetime import date, datetime
from typing import List, Dict
from six import iteritems
from ..util import deserialize_date, deserialize_datetime
import flask

import job_manager


def _job_to_cwl_job(job):
    return Job(
            id=job.id,
            name=job.name,
            workflow=job.workflow,
            input=job.input,
            state=job.get_state(),
            output={},
            log=flask.url_for('.swagger_server_controllers_default_controller_get_job_log_by_id',
                jobId=job.id,
                _external=True)
        )




def cancel_job_by_id(jobId):
    """
    Cancel a job

    :param jobId: Job ID
    :type jobId: str

    :rtype: Job
    """

    job = job_manager.job_store().get_job(jobId)
    if not job:
        flask.abort(404, "Job not found")

    job.cancel()

    return flask.url_for('.swagger_server_controllers_default_controller_get_job_by_id',
            jobId=job.id,
            _external=True)


def delete_job_by_id(jobId):
    """
    Deleta a job
    Delete a job, if job is in waiting or running state then job will be cancelled first.
    :param jobId: Job ID
    :type jobId: str

    :rtype: None
    """
    job_manager.job_store().delete_job(jobId)
    return None, 204


def get_job_by_id(jobId):
    """
    Get a job

    :param jobId: Job ID
    :type jobId: str

    :rtype: Job
    """
    job = job_manager.job_store().get_job(jobId)
    if not job:
        flask.abort(404, "Job not found")

    return _job_to_cwl_job(job)


def get_job_log_by_id(jobId):
    """
    Log of a job

    :param jobId: Job ID
    :type jobId: str

    :rtype: str
    """
    return job_manager.job_store().get_job(jobId).get_log()


def get_jobs():
    """
    list of jobs
    get a list of all jobs, running, cancelled, or otherwise.

    :rtype: List[Job]
    """

    job_list = job_manager.job_store().list_jobs()

    return [_job_to_cwl_job(job) for job in job_list]

def post_job(body):
    """
    submit a new job
    Submit a new job from a workflow definition.
    :param body: Input binding for workflow.
    :type body: dict | bytes

    :rtype: Job
    """
    if connexion.request.is_json:
        body = JobDescription.from_dict(connexion.request.get_json())

    job_id = job_manager.job_store().create_job(
        job_manager.job_description.JobDescription(
            name=body.name,
            workflow=body.workflow,
            input=body.input
            )
        )

    job_manager.job_runner().start_job(job_id)

    job = job_manager.job_store().get_job(job_id)
    return _job_to_cwl_job(job)
