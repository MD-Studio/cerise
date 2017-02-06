import connexion
from swagger_server.models.job import Job
from swagger_server.models.job_description import JobDescription
from datetime import date, datetime
from typing import List, Dict
from six import iteritems
from ..util import deserialize_date, deserialize_datetime


def cancel_job_by_id(jobId):
    """
    Cancel a job
    
    :param jobId: Job ID
    :type jobId: str

    :rtype: Job
    """
    return 'do some magic!'


def delete_job_by_id(jobId):
    """
    Deleta a job
    Delete a job, if job is in waiting or running state then job will be cancelled first.
    :param jobId: Job ID
    :type jobId: str

    :rtype: None
    """
    return 'do some magic!'


def get_job_by_id(jobId):
    """
    Get a job
    
    :param jobId: Job ID
    :type jobId: str

    :rtype: Job
    """
    return 'do some magic!'


def get_job_log_by_id(jobId):
    """
    Log of a job
    
    :param jobId: Job ID
    :type jobId: str

    :rtype: str
    """
    return 'do some magic!'


def get_jobs():
    """
    list of jobs
    get a list of all jobs, running, cancelled, or otherwise.

    :rtype: List[Job]
    """
    return 'do some magic!'


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
    return 'do some magic!'
