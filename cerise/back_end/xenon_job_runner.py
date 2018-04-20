import jpype
import logging
import os
import xenon

from cerise.job_store.job_state import JobState

from time import sleep

class XenonJobRunner:
    def __init__(self, job_store, xenon, config, api_files_path, api_install_script_path):
        """Create a XenonJobRunner object.

        Args:
            job_store (JobStore): The job store to get jobs from.
            config (Config): The configuration.
        """
        self._logger = logging.getLogger(__name__)
        """Logger: The logger for this class."""
        self._job_store = job_store
        """The JobStore to obtain jobs from."""
        self._x = xenon
        """The Xenon instance to use."""
        self._username = None
        """The remote user to connect as."""
        self._api_files_path = api_files_path
        """str: The remote path of the API files directory."""
        self._remote_cwlrunner = None
        """str: The remote path to the cwl runner executable."""
        self._sched = config.get_scheduler()
        """The Xenon scheduler to start jobs through."""
        self._queue_name = config.get_queue_name()
        """The name of the remote queue to submit jobs to."""
        self._mpi_slots_per_node = config.get_slots_per_node()
        """Number of MPI slots per node to request."""

        self._logger.debug('Slots per node set to ' + str(self._mpi_slots_per_node))

        self._remote_cwlrunner = config.get_remote_cwl_runner()

        if self._username is not None:
            self._remote_cwlrunner = self._remote_cwlrunner.replace('$CERISE_USERNAME', self._username)

        self._remote_cwlrunner = self._remote_cwlrunner.replace('$CERISE_API_FILES', self._api_files_path)

        if api_install_script_path is not None:
            self._run_api_install_script(config,
                    self._api_files_path, api_install_script_path)

    def _run_api_install_script(self, config, api_files_path, api_install_script_path):
        sched = config.get_scheduler(run_on_head_node=True)
        xenon_jobdesc = xenon.jobs.JobDescription()
        xenon_jobdesc.setWorkingDirectory(api_files_path)
        xenon_jobdesc.setExecutable(api_install_script_path)
        xenon_jobdesc.setArguments([api_files_path])
        # Not supported with SSH adapter, waiting for Xenon 2...
        # xenon_jobdesc.addEnvironment('CERISE_API_FILES', api_files_path)
        self._logger.debug("Starting api install script " + api_install_script_path)
        xenon_job = self._x.jobs().submitJob(sched, xenon_jobdesc)

        status = self._x.jobs().getJobStatus(xenon_job)
        delay = 1
        while not status.isDone():
            sleep(delay)
            if delay < 5:
                delay += 1
            status = self._x.jobs().getJobStatus(xenon_job)
        self._logger.debug("API install script done")

    def update_job(self, job_id):
        """Get status from Xenon and update store.

        Args:
            job_id (str): ID of the job to get the status of.
        """
        self._logger.debug("Updating job " + job_id + " from remote job")
        with self._job_store:
            job = self._job_store.get_job(job_id)
            active_jobs = self._x.jobs().getJobs(self._sched, [])
            xenon_job = [x_job for x_job in active_jobs if x_job.getIdentifier() == job.remote_job_id]
            if len(xenon_job) == 1:
                xenon_status = self._x.jobs().getJobStatus(xenon_job[0])
                if xenon_status.isRunning():
                    job.try_transition(JobState.WAITING, JobState.RUNNING)
                    job.try_transition(JobState.WAITING_CR, JobState.RUNNING_CR)
                    return
                if not xenon_status.isDone():
                    # Still waiting in the queue, check again later
                    return

            # Not running or waiting, so it's finished unless we cancelled it
            job.try_transition(JobState.WAITING, JobState.FINISHED)
            job.try_transition(JobState.RUNNING, JobState.FINISHED)
            job.try_transition(JobState.WAITING_CR, JobState.CANCELLED)
            job.try_transition(JobState.RUNNING_CR, JobState.CANCELLED)

    def start_job(self, job_id):
        """Get a job from the job store and start it on the compute resource.

        Args:
            job_id (str): The id of the job to start.
        """
        self._logger.debug('Starting job ' + job_id)
        with self._job_store:
            job = self._job_store.get_job(job_id)
            # submit job
            xenon_jobdesc = xenon.jobs.JobDescription()
            xenon_jobdesc.setWorkingDirectory(job.remote_workdir_path)
            xenon_jobdesc.setExecutable('bash')
            # Work around Xenon issue #601
            args = [
                    '-c',
                    self._remote_cwlrunner +
                    ' ' + job.remote_workflow_path +
                    ' ' + job.remote_input_path +
                    ' >{}'.format(job.remote_stdout_path) +
                    ' 2>{}'.format(job.remote_stderr_path)
                    ]
            xenon_jobdesc.setArguments(args)
            # xenon_jobdesc.setStdout(job.remote_stdout_path)
            # xenon_jobdesc.setStderr(job.remote_stderr_path)
            xenon_jobdesc.setMaxTime(60)
            if self._queue_name:
                xenon_jobdesc.setQueueName(self._queue_name)
            xenon_jobdesc.setProcessesPerNode(self._mpi_slots_per_node)
            xenon_jobdesc.setStartSingleProcess(True)
            print("Starting job: " + str(xenon_jobdesc))
            xenon_job = self._x.jobs().submitJob(self._sched, xenon_jobdesc)
            job.remote_job_id = xenon_job.getIdentifier()
            self._logger.debug('Job submitted')

        try:
            sleep(1)    # work-around for Xenon local running bug
        except KeyboardInterrupt:
            pass        # exit gracefully

    def cancel_job(self, job_id):
        """Cancel a running job.

        Job must be cancellable, i.e. in JobState.RUNNING or
        JobState.WAITING. If it isn't cancellable, this
        function does nothing.

        Cancellation may not happen immediately. If the cancellation
        request has been executed immediately and the job is now gone,
        this function returns False. If the job will be cancelled soon,
        it returns True.

        Args:
            job_id (str): The id of the job to cancel.

        Returns:
            bool: Whether the job is still running.
        """
        XenonException = xenon.nl.esciencecenter.xenon.XenonException

        self._logger.debug('Cancelling job ' + job_id)
        with self._job_store:
            job = self._job_store.get_job(job_id)
            if JobState.is_remote(job.state):
                active_jobs = self._x.jobs().getJobs(self._sched, [])
                xenon_job = [x_job for x_job in active_jobs if x_job.getIdentifier() == job.remote_job_id]
                if len(xenon_job) == 1:
                    status = self._x.jobs().getJobStatus(xenon_job[0])
                    if status.isRunning():
                        try:
                            new_state = self._x.jobs().cancelJob(xenon_job[0])
                        except jpype.JException(XenonException):
                            return False
                        return bool(new_state.isRunning())
        return False
