import connexion
from swagger_server.models.job import Job
from swagger_server.models.job_description import JobDescription
import flask
import json

import job_manager
from job_manager import job_state

def _internal_job_to_rest_job(job):
    job_output = json.loads(getattr(job, 'local_output', '{}'))
    job_input = json.loads(job.local_input)

    return Job(
            id=job.id,
            name=job.name,
            workflow=job.workflow,
            input=job_input,
            state=job_state.JobState.to_cwl_state_string(job.state),
            output=job_output,
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
    with job_manager.job_store():
        job = job_manager.job_store().get_job(jobId)
        if not job:
            flask.abort(404, "Job not found")

        job_manager.job_runner().cancel_job(jobId)
        job_manager.job_runner().update_job(jobId)

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
    with job_manager.job_store():
        job_manager.job_runner().cancel_job(jobId)
        # Wait until it is gone?
        job_manager.remote_files().delete_job(jobId)
        job_manager.local_files().delete_output_dir(jobId)
        job_manager.job_store().delete_job(jobId)
    return None, 204


def get_job_by_id(jobId):
    """
    Get a job

    :param jobId: Job ID
    :type jobId: str

    :rtype: Job
    """
    with job_manager.job_store():
        job = job_manager.job_store().get_job(jobId)
        if not job:
            flask.abort(404, "Job not found")

        job_manager.job_runner().update_job(jobId)
        output_files = job_manager.remote_files().update_job(jobId)
        job_manager.local_files().publish_job_output(jobId, output_files)

    return _internal_job_to_rest_job(job)


def get_job_log_by_id(jobId):
    """
    Log of a job

    :param jobId: Job ID
    :type jobId: str

    :rtype: str
    """
    with job_manager.job_store():
        job_manager.remote_files().update_job(jobId)
        return job_manager.job_store().get_job(jobId).log


def get_jobs():
    """
    list of jobs
    get a list of all jobs, running, cancelled, or otherwise.

    :rtype: List[Job]
    """

    with job_manager.job_store():
        job_list = job_manager.job_store().list_jobs()
        for job in job_list:
            job_manager.job_runner().update_job(job.id)
            output_files = job_manager.remote_files().update_job(job.id)
            job_manager.local_files().publish_job_output(job.id, output_files)

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

    with job_manager.job_store():
        job_id = job_manager.job_store().create_job(
            job_manager.job_description.JobDescription(
                name=body.name,
                workflow=body.workflow,
                job_input=body.input
                )
            )

        job = job_manager.job_store().get_job(job_id)

        if job.try_transition(job_state.JobState.SUBMITTED, job_state.JobState.STAGING):
            input_files = job_manager.local_files().resolve_input(job_id)
            job_manager.remote_files().stage_job(job_id, input_files)
            job_manager.local_files().create_output_dir(job_id)
            job_manager.job_runner().start_job(job_id)
        else:
            job.state = job_state.JobState.SYSTEM_ERROR

        return _internal_job_to_rest_job(job)
