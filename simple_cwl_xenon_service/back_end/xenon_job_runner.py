import jpype
import xenon

from simple_cwl_xenon_service.job_store.job_state import JobState

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
        self._remote_cwlrunner = None
        """str: The remote path to the cwl runner executable."""
        self._sched = None
        """The Xenon scheduler to start jobs through."""

        self._remote_cwlrunner = xenon_config['jobs'].get('cwl-runner', 'cwl-runner')
        self._make_scheduler(xenon_config)


    def _make_scheduler(self, xenon_config):
        scheme = xenon_config['jobs'].get('scheme', 'local')
        location = xenon_config['jobs'].get('location', '')
        if 'username' in xenon_config['jobs']:
            username = xenon_config['jobs']['username']
            password = xenon_config['jobs']['password']
            jpassword = jpype.JArray(jpype.JChar)(len(password))
            for i in range(len(password)):
                jpassword[i] = password[i]
            credential = self._x.credentials().newPasswordCredential(
                    scheme, username, jpassword, None)
            self._sched = self._x.jobs().newScheduler(
                    scheme, location, credential, None)
        else:
            self._sched = self._x.jobs().newScheduler(
                    scheme, location, None, None)


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
                if len(xenon_job) == 1:
                    xenon_status = self._x.jobs().getJobStatus(xenon_job[0])
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
            xenon_jobdesc.setExecutable(self._remote_cwlrunner)
            args = [
                    job.remote_workflow_path,
                    job.remote_input_path
                    ]
            xenon_jobdesc.setArguments(args)
            xenon_jobdesc.setStdout(job.remote_stdout_path)
            xenon_jobdesc.setStderr(job.remote_stderr_path)
            xenon_job = self._x.jobs().submitJob(self._sched, xenon_jobdesc)
            job.remote_job_id = xenon_job.getIdentifier()
            if not (job.try_transition(JobState.STAGING, JobState.WAITING) or
                    job.try_transition(JobState.STAGING_CR, JobState.WAITING_CR)):
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
