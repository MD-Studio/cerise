from cerise.job_store.sqlite_job_store import SQLiteJobStore
from cerise.job_store.job_state import JobState
from cerise.back_end.cwl import get_cwltool_result, is_workflow
from cerise.back_end.local_files import ConnectionError, LocalFiles
from cerise.back_end.remote_api import RemoteApi
from cerise.back_end.remote_job_files import RemoteJobFiles
from cerise.back_end.job_planner import InvalidJobError, JobPlanner
from cerise.back_end.job_runner import JobRunner
from cerise.config import Config
from cerise.job_store.sqlite_job import SQLiteJob

import logging
from paramiko.ssh_exception import SSHException     # type: ignore
import time
import traceback
from typing import cast


class ExecutionManager:
    """Handles the execution of jobs on the remote resource.
    The execution manager monitors the job store for files that are
    ready to be staged in, started, cancelled, staged out, or deleted,
    and performs the required activity. It also monitors the remote
    resource, ensuring that any remote state changes are propagated to
    the job store correctly.
    """
    def __init__(self, config: Config, local_api_dir: str) -> None:
        """Set up the execution manager.

        Args:
            config: The configuration.
            local_api_dir: The path to the local API directory.
        """
        self._logger = logging.getLogger(__name__)

        self._update_available = False
        """Whether the installed API is older than the local one."""

        self._shutting_down = False
        """True iff we're shutting down."""

        self._job_store = SQLiteJobStore(config.get_database_location())
        """The job store to use."""
        self._local_files = LocalFiles(self._job_store, config)
        """The local files manager."""
        self._remote_api = RemoteApi(config, local_api_dir)
        """The remote API manager."""
        self._remote_refresh = config.get_remote_refresh()

        self._job_planner = JobPlanner(self._job_store, local_api_dir)
        """Determines required hardware resources."""

        self._remote_job_files = RemoteJobFiles(self._job_store, config)
        """The remote job files manager."""

        remote_cwlrunner = self._remote_api.translate_runner_location(
                config.get_remote_cwl_runner())
        self._job_runner = JobRunner(
                self._job_store, config, remote_cwlrunner)
        """The job runner submits jobs and checks on them."""

        # recover database from crash
        with self._job_store:
            for job in self._job_store.list_jobs():
                if job.state == JobState.STAGING_IN:
                    self._remote_job_files.delete_job(job.id)
                    job.state = JobState.SUBMITTED
                if job.state == JobState.STAGING_OUT:
                    self._local_files.delete_output_dir(job.id)
                    job.state = JobState.FINISHED
                if job.state == JobState.WAITING_CR:
                    self._job_runner.cancel_job(job.id)
                if job.state == JobState.RUNNING_CR:
                    self._job_runner.cancel_job(job.id)

        # Check for updates
        self._update_available = self._remote_api.update_available()
        if self._update_available:
            self._logger.info('Specialisation update available')

        self._logger.info('Started back-end')

    def shutdown(self) -> None:
        """Requests the execution manager to execute a clean shutdown."""
        self._logger.debug('Shutdown requested')
        self._shutting_down = True

    def _delete_job(self, job_id: str, job: SQLiteJob) -> None:
        """Delete a job.

        Deletes the job from the compute resource, and if it was
        destaged, also from the local file store.

        Prerequisite: the job is in a final state.

        Args:
            job_id: The id of the job
            job: The job object
        """
        self._logger.debug('Deleting job ' + job_id)
        self._remote_job_files.delete_job(job_id)
        if job.state == JobState.SUCCESS:
            self._local_files.delete_output_dir(job_id)
        self._job_store.delete_job(job_id)

    def _cancel_job(self, job_id: str, job: SQLiteJob) -> None:
        """Cancel a job.

        If the job is running, the cancellation request may take some
        time to process by the compute resource. In this case, the job
        will remain in RUNNING_CR. Otherwise, it will be cancelled
        immediately, and be put in CANCELLED.

        Precondition: Job is in a _CR state.
        Postcondition: Job is in CANCELLED or RUNNING_CR.

        Args:
            job_id: The id of the job
            job: The job object
        """
        job.info('Cancelling job')
        if self._job_runner.cancel_job(job_id):
            job.state = JobState.RUNNING_CR
        else:
            job.state = JobState.CANCELLED
            job.info('Job cancelled')

    def _stage_and_start_job(self, job_id: str, job: SQLiteJob) -> None:
        """Stages, plans and starts a job.

        Precondition: Job is in STAGING_IN state
        Postcondition: Job is in WAITING, PERMANENT_FAILURE, CANCELLED,
                or WAITING_CR

        Args:
            job_id: The id of the job
            job: The job object
        """
        try:
            job.info('Resolving inputs')
            input_files = self._local_files.resolve_input(job_id)
        except FileNotFoundError as e:
            job.error('Input not found: {}'.format(e.args[0]))
            job.state = JobState.PERMANENT_FAILURE
            return
        except ValueError as e:
            job.error('Invalid input: {}'.format(e.args[0]))
            job.state = JobState.PERMANENT_FAILURE
            return
        except ConnectionError:
            job.resolve_retry_count += 1
            job.warning('Could not connect to input source, will retry')
            job.state = JobState.SUBMITTED
            if job.resolve_retry_count > 10:
                job.error('Could not connect to input source, giving up')
                job.state = JobState.TEMPORARY_FAILURE
            return

        if not is_workflow(cast(bytes, job.workflow_content)):
            job.error('Input is not a CWL workflow')
            job.state = JobState.PERMANENT_FAILURE
            return

        if job.try_transition(JobState.STAGING_IN_CR, JobState.CANCELLED):
            job.info('Job was cancelled while resolving input')
            return

        job.info('Resolved input, now planning')
        try:
            self._job_planner.plan_job(job_id)
        except InvalidJobError:
            job.error('Job is invalid')
            job.state = JobState.PERMANENT_FAILURE
            return

        if job.state == JobState.PERMANENT_FAILURE:
            return

        job.info('Planned job, now staging in inputs')
        workflow_content = self._remote_api.translate_workflow(cast(bytes, job.workflow_content))
        try:
            self._remote_job_files.stage_job(job_id, input_files, workflow_content)
        except SSHException as e:
            job.warning('Connection problem with remote resource: {}'.format(e.args[0]))
            job.warning('Will try again later')
            job.state = JobState.SUBMITTED
            return
        except IOError as e:
            job.error('An IO error occurred while uploading the job'
                      ' input data: {}. Please check that your network'
                      ' connection works, and that you have enough'
                      ' disk space or quota on the remote machine.'
                      ''.format(e))
            job.state = JobState.SYSTEM_ERROR
            return

        job.info('Staged job, now starting')
        job.info('API versions:')
        for project_version in self._remote_api.get_projects():
            job.info('  {}'.format(project_version))
        try:
            self._job_runner.start_job(job_id)
        except SSHException as e:
            job.warning('Connection problem with remote resource: {}'.format(e.args[0]))
            job.warning('Will try again later')
            job.state = JobState.SUBMITTED
            return
        job.info('Started job')

        if not (job.try_transition(JobState.STAGING_IN, JobState.WAITING) or
                job.try_transition(JobState.STAGING_IN_CR, JobState.WAITING_CR)):
            self._logger.critical('Something odd happened while staging and starting')
            self._logger.critical('State is now {}'.format(job.state))
            job.state = JobState.SYSTEM_ERROR

    def _destage_job(self, job_id: str, job: SQLiteJob) -> None:
        """Get job results back from the compute resource.

        Precondition: Job is in FINISHED
        Postcondition: Job is in SUCCESS, TEMPORARY_FAILURE, PERMANENT_FAILURE
                or CANCELLED

        Args:
            job_id: The job's id
            job: The job object
        """
        result = get_cwltool_result(job.remote_error)

        if job.try_transition(JobState.FINISHED, JobState.STAGING_OUT):
            job.info('Starting destaging of results')
            try:
                output_files = self._remote_job_files.destage_job_output(job_id)
                self._local_files.publish_job_output(job_id, output_files)
            except SSHException as e:
                job.warning('Connection problem with remote resource: {}'.format(e.args[0]))
                job.warning('Will try again later')
                job.state = JobState.FINISHED
                return

            job.info('Results downloaded and available')

            if not (job.try_transition(JobState.STAGING_OUT, result) or
                    job.try_transition(JobState.STAGING_OUT_CR, JobState.CANCELLED)):
                job.state = JobState.SYSTEM_ERROR

    def _process_jobs(self, check_remote: bool) -> bool:
        """
        Go through the jobs and do what needs to be done.

        Args:
            check_remote: Whether to access the remote
                compute resource to check on jobs.

        Returns:
            True iff there are currently running jobs.
        """
        # If we don't check remote, assume that we have running jobs,
        # so that we don't install updates while jobs are running.
        have_running_jobs = not check_remote

        jobs = self._job_store.list_jobs()
        for job_id in [job.id for job in jobs]:
            if self._shutting_down:
                break

            try:
                job = self._job_store.get_job(job_id)
                self._logger.debug('Processing job ' + job_id + ' with current state ' + job.state.value)

                if check_remote and JobState.is_remote(job.state):
                    self._logger.debug('Checking remote state')
                    try:
                        self._job_runner.update_job(job_id)
                        self._remote_job_files.update_job(job_id)
                        job = self._job_store.get_job(job_id)
                        have_running_jobs = have_running_jobs or JobState.is_remote(job.state)
                    except SSHException:
                        have_running_jobs = True

                if job.state == JobState.FINISHED:
                    self._destage_job(job_id, job)

                if not self._update_available:
                    if job.try_transition(JobState.SUBMITTED, JobState.STAGING_IN):
                        self._stage_and_start_job(job_id, job)
                        self._logger.debug('Staged and started job')

                if JobState.cancellation_active(job.state):
                    self._cancel_job(job_id, job)

                self._logger.debug('State is now ' + job.state.value)

                if job.please_delete and JobState.is_final(job.state):
                    self._delete_job(job_id, job)
            except:
                job.state = JobState.SYSTEM_ERROR
                self._logger.critical('An internal error occurred when processing job ' + job.id)
                self._logger.critical(traceback.format_exc())

        return have_running_jobs

    def execute_jobs(self) -> None:
        """Run the main backend execution loop.

        This repeatedly processes jobs, but does not check the remote
        compute resource more often than specified in the
        remote_refresh configuration parameter.
        """
        with self._job_store:
            last_active = time.perf_counter() - self._remote_refresh - 1
            # Handler in run_back_end throws KeyboardInterrupt in order to
            # break the sleep call; catch it to exit gracefully
            try:
                while not self._shutting_down:
                    now = time.perf_counter()
                    check_remote = now - last_active > self._remote_refresh

                    have_running_jobs = self._process_jobs(check_remote)
                    if not have_running_jobs and self._update_available:
                        self._remote_api.install()
                        self._update_available = False

                    if check_remote:
                        last_active = time.perf_counter()

                    time.sleep(0.1)

            except KeyboardInterrupt:
                pass
        self._logger.debug('Shutting down')
