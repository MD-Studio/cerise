import xenon

from .job_state import JobState

from time import sleep

class XenonJobRunner:
    def __init__(self, job_store, xenon, xenon_config):
        """Create a XenonJobRunner object.

        Args:
            job_store (JobStore): The job store to get jobs from.
            xenon_config (Dict): A dict containing key-value pairs with Xenon
                configuration.
        """
        self._job_store = job_store
        """The JobStore to obtain jobs from."""
        self._x = xenon
        """The Xenon instance to use."""
        self._sched = self._x.jobs().newScheduler(
                xenon_config['jobs'].get('scheme', 'local'),
                xenon_config['jobs'].get('location'),
                xenon_config['jobs'].get('credential'),
                xenon_config['jobs'].get('properties')
                )
        """The Xenon scheduler to start jobs through."""

    def update_job(self, job_id):
        """Get status from Xenon and update store.

        Args:
            job_id (str): ID of the job to get the status of.
        """
        with self._job_store:
            job = self._job_store.get_job(job_id)

            # get state
            if JobState.is_remote(job.state):
                active_jobs = self._x.jobs().getJobs(self._sched, [])
                xenon_job = [x_job for x_job in active_jobs if x_job.getIdentifier() == job.remote_job_id]
                print("Xenon job:")
                print(xenon_job)
                if len(xenon_job) == 1:
                    xenon_status = self._x.jobs().getJobStatus(xenon_job[0])
                    print("Xenon status:")
                    print(xenon_status)
                    if xenon_status.isRunning():
                        job.try_transition(JobState.WAITING, JobState.RUNNING)
                        job.try_transition(JobState.WAITING_CR, JobState.RUNNING_CR)
                    elif xenon_status.isDone():
                        job.try_transition(JobState.WAITING, JobState.FINISHED)
                        job.try_transition(JobState.RUNNING, JobState.FINISHED)
                        job.try_transition(JobState.WAITING_CR, JobState.CANCELLED)
                        job.try_transition(JobState.RUNNING_CR, JobState.CANCELLED)
                else:
                    if JobState.cancellation_active(job.state):
                        job.state = JobState.CANCELLED
                    else:
                        job.state = JobState.FINISHED

    def start_job(self, job_id):
        """Get a job from the job store and start it on the compute resource.

        Args:
            job_id (str): The id of the job to start.
        """
        with self._job_store:
            job = self._job_store.get_job(job_id)
            # submit job
            xenon_jobdesc = xenon.jobs.JobDescription()
            xenon_jobdesc.setWorkingDirectory(job.remote_workdir_path)
            xenon_jobdesc.setExecutable('cwl-runner')
            args = [
                    job.remote_workflow_path,
                    job.remote_input_path
                    ]
            xenon_jobdesc.setArguments(args)
            xenon_jobdesc.setStdout(job.remote_stdout_path)
            xenon_jobdesc.setStderr(job.remote_stderr_path)
            xenon_job = self._x.jobs().submitJob(self._sched, xenon_jobdesc)
            job.remote_job_id = xenon_job.getIdentifier()
            if not job.try_transition(JobState.STAGING, JobState.WAITING):
                job.state = JobState.SYSTEM_ERROR

        sleep(1)    # work-around for Xenon local running bug

    def cancel_job(self, job_id):
        """Cancel a running job.

        Job must be cancellable, i.e. in JobState.RUNNING or
        JobState.WAITING. If it isn't cancellable, this
        function does nothing.

        Args:
            job_id (str): The id of the job to cancel.
        """
        with self._job_store:
            job = self._job_store.get_job(job_id)
            if JobState.is_remote(job.state):
                active_jobs = self._x.jobs().getJobs(self._sched, [])
                xenon_job = [x_job for x_job in active_jobs if x_job.getIdentifier() == job.remote_job_id]
                if len(xenon_job) == 1:
                    new_state = self._x.jobs().cancelJob(xenon_job[0])
                    if new_state.isRunning():
                        job.try_transition(JobState.WAITING, JobState.WAITING_CR)
                        job.try_transition(JobState.RUNNING, JobState.RUNNING_CR)
                    else:
                        job.state = JobState.CANCELLED

def _xenon_status_to_job_state(xenon_status):
    """Convert a xenon JobStatus to our JobState.

    Args:
        xenon_status (JobStatus): A xenon JobStatus object.

    Returns:
        JobState: A corresponding JobState object.
    """
    if xenon_status.isRunning():
        return JobState.RUNNING

    if xenon_status.isDone():
        if xenon_status.hasException():
            # TODO: fix, check that it is a JobCanceledException
            print(xenon_status.getException())
            return JobState.CANCELLED

        exit_code = xenon_status.getExitCode().intValue()
        if exit_code == 0:
            return JobState.SUCCESS
        if exit_code == 1:
            return JobState.PERMANENT_FAILURE
        if exit_code == 33:
            return JobState.PERMANENT_FAILURE
        return JobState.SYSTEM_ERROR

