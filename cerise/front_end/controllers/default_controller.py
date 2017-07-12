import connexion
from front_end.models.job import Job
from front_end.models.job_description import JobDescription
import flask
import json

from cerise.job_store import job_state
from cerise.job_store.sqlite_job_store import SQLiteJobStore
from cerise.config import config

_job_store = SQLiteJobStore(config['database']['file'])

def _internal_job_to_rest_job(job):
    if job.local_output == '':
        job_output = {}
    else:
        job_output = json.loads(job.local_output)
    job_input = json.loads(job.local_input)

    return Job(
#            id=flask.url_for('.front_end_controllers_default_controller_get_job_by_id',
#                jobId=job.id,
#                _external=True),
            id = job.id,
            name=job.name,
            workflow=job.workflow,
            input=job_input,
            state=job_state.JobState.to_cwl_state_string(job.state),
            output=job_output,
            log=flask.url_for('.front_end_controllers_default_controller_get_job_log_by_id',
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
    with _job_store:
        job = _job_store.get_job(jobId)
        if not job:
            flask.abort(404, "Job not found")

        job.try_transition(job_state.JobState.SUBMITTED, job_state.JobState.CANCELLED)
        job.try_transition(job_state.JobState.STAGING, job_state.JobState.STAGING_CR)
        job.try_transition(job_state.JobState.WAITING, job_state.JobState.WAITING_CR)
        job.try_transition(job_state.JobState.RUNNING, job_state.JobState.RUNNING_CR)
        job.try_transition(job_state.JobState.FINISHED, job_state.JobState.CANCELLED)
        job.try_transition(job_state.JobState.DESTAGING, job_state.JobState.DESTAGING_CR)

    return get_job_by_id(jobId)


def delete_job_by_id(jobId):
    """
    Deleta a job
    Delete a job, if job is in waiting or running state then job will be cancelled first.
    :param jobId: Job ID
    :type jobId: str

    :rtype: None
    """
    with _job_store:
        cancel_job_by_id(jobId)
        job = _job_store.get_job(jobId)
        job.please_delete = True
    return None, 204


def get_job_by_id(jobId):
    """
    Get a job

    :param jobId: Job ID
    :type jobId: str

    :rtype: Job
    """
    with _job_store:
        job = _job_store.get_job(jobId)
        if not job:
            flask.abort(404, "Job not found")

        return _internal_job_to_rest_job(job), 200


def get_job_log_by_id(jobId):
    """
    Log of a job

    :param jobId: Job ID
    :type jobId: str

    :rtype: str
    """
    with _job_store:
        job = _job_store.get_job(jobId)
        if not job:
            flask.abort(404, "Job not found")
        return job.log


def get_jobs():
    """
    list of jobs
    get a list of all jobs, running, cancelled, or otherwise.

    :rtype: List[Job]
    """

    with _job_store:
        job_list = _job_store.list_jobs()
        return [_internal_job_to_rest_job(job) for job in job_list]

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

    with _job_store:
        job_id = _job_store.create_job(
                body.name, body.workflow, json.dumps(body.input))

        job = _job_store.get_job(job_id)
        return _internal_job_to_rest_job(job), 201
