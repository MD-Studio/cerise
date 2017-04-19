import jpype
import requests
import xenon
from xenon.files import OpenOption

from .job_state import JobState

from time import sleep

class XenonJobRunner:
    def __init__(self, job_store, xenon, xenon_config={}):
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
            if (   job.state == JobState.WAITING
                    or job.state == JobState.RUNNING
                    ):
                try:
                    xenon_job = job.runner_data
                    xenon_status = self._x.jobs().getJobStatus(xenon_job)
                    job.state = self._xenon_status_to_job_state(xenon_status)
                except xenon.exceptions.XenonException:
                    # Xenon does not know about this job anymore
                    # We should be able to get a status once after the job
                    # finishes, so something went wrong
                    print('Job disappeared?')
                    job.state = JobState.SYSTEM_ERROR
                    pass

    def update_all_jobs(self):
        """Get status from Xenon and update store, for all jobs.
        """
        with self._job_store:
            for job in self._job_store.list_jobs():
                self.update_job(job.id)

    def start_job(self, job_id):
        """Get a job from the job store and start it on the compute resource.

        Args:
            job_id (str): The id of the job to start.
        """
        with self._job_store:
            job = self._job_store.get_job(job_id)
            print(job.workdir_path)
            # submit job
            xenon_jobdesc = xenon.jobs.JobDescription()
            xenon_jobdesc.setWorkingDirectory(job.workdir_path)
            xenon_jobdesc.setExecutable('cwl-runner')
            args = [
                    job.workflow_path,
                    job.input_path
                    ]
            xenon_jobdesc.setArguments(args)
            xenon_jobdesc.setStdout(job.stdout_path)
            xenon_jobdesc.setStderr(job.stderr_path)
            xenon_job = self._x.jobs().submitJob(self._sched, xenon_jobdesc)
            job.runner_data = xenon_job
            job.state = JobState.WAITING

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
            if JobState.is_cancellable(job.state):
                xenon_job = job.runner_data
                new_status = self._x.jobs().cancelJob(xenon_job)
                job.state = JobState.CANCELLED

    def _xenon_status_to_job_state(self, xenon_status):
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

