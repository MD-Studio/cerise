from cerise.job_store.sqlite_job_store import SQLiteJobStore
from cerise.job_store.job_state import JobState
from .cwl import get_cwltool_result
from .cwl import is_workflow
from .local_files import LocalFiles
from .xenon_remote_files import XenonRemoteFiles
from .xenon_job_runner import XenonJobRunner

import logging
import time
import traceback

class ExecutionManager:
    """Handles the execution of jobs on the remote resource.
    The execution manager monitors the job store for files that are
    ready to be staged in, started, cancelled, staged out, or deleted,
    and performs the required activity. It also monitors the remote
    resource, ensuring that any remote state changes are propagated to
    the job store correctly.
    """
    def __init__(self, config, api_config, apidir, xenon):
        """Set up the execution manager.

        Args:
            config (Dict): A document containing the general cerise
                configuration
            api_config (Dict): A document containing the API
                configuration (from the specialisation)
            apidir (str): The remote path to the remote API directory.
            xenon (Xenon): The Xenon object to use.
        """
        self._logger = logging.getLogger(__name__)

        self._shutting_down = False

        # _job_store = InMemoryJobStore()
        self._job_store = SQLiteJobStore(config['database']['file'])
        """SQLiteJobStore: The job store to use."""
        self._local_files = LocalFiles(self._job_store, config['client-file-exchange'])
        """LocalFiles: The local files manager."""
        self._remote_files = XenonRemoteFiles(self._job_store, xenon, api_config['compute-resource'])
        """RemoteFiles: The remote files manager."""
        self._remote_refresh = api_config['compute-resource'].get('refresh', 2)

        api_install_script_path, api_files_path = self._remote_files.stage_api(apidir)

        # TODO: recover database from crash
        with self._job_store:
            for job in self._job_store.list_jobs():
                if job.state == JobState.STAGING_IN:
                    self._remote_files.delete_job(job.id)
                    job.state = JobState.SUBMITTED
                if job.state == JobState.STAGING_OUT:
                    self._local_files.delete_output_dir(job.id)
                    job.state = JobState.FINISHED

        # if job is in WAITING, it may never have been started
        # should check somehow whether a job in WAITING actually
        # was submitted to the compute resource

        # for each job in WAITING_CR or RUNNING_CR
            # if it's running
                # send cancel request

        self._job_runner = XenonJobRunner(
                self._job_store, xenon,
                api_config['compute-resource'],
                api_files_path, api_install_script_path)
        self._logger.info('Started back-end')

    def shutdown(self):
        """Requests the execution manager to execute a clean shutdown."""
        self._logger.debug('Shutdown requested')
        self._shutting_down = True

    def _delete_job(self, job_id, job):
        self._logger.debug('Deleting job ' + job_id)
        self._remote_files.delete_job(job_id)
        if job.state == JobState.SUCCESS:
            self._local_files.delete_output_dir(job_id)
        self._job_store.delete_job(job_id)

    def _cancel_job(self, job_id, job):
        if self._job_runner.cancel_job(job_id):
            job.state = JobState.RUNNING_CR
        else:
            job.state = JobState.CANCELLED

    def _stage_and_start_job(self, job_id, job):
        try:
            input_files = self._local_files.resolve_input(job_id)
        except FileNotFoundError:
            job.state = JobState.PERMANENT_FAILURE
            return

        if not is_workflow(job.workflow_content):
            job.state = JobState.PERMANENT_FAILURE
            return

        if job.try_transition(JobState.STAGING_IN_CR, JobState.CANCELLED):
            self._logger.debug('Job was cancelled while resolving input')
            return
        self._remote_files.stage_job(job_id, input_files)
        self._job_runner.start_job(job_id)
        if not (job.try_transition(JobState.STAGING_IN, JobState.WAITING) or
                job.try_transition(JobState.STAGING_IN_CR, JobState.WAITING_CR)):
            job.state = JobState.SYSTEM_ERROR

    def _destage_job(self, job_id, job):
        result = get_cwltool_result(job.log)

        if result == JobState.SUCCESS:
            if job.try_transition(JobState.FINISHED, JobState.STAGING_OUT):
                output_files = self._remote_files.destage_job_output(job_id)
                if output_files is not None:
                    self._local_files.publish_job_output(job_id, output_files)
                    job.try_transition(JobState.STAGING_OUT, JobState.SUCCESS)
                    job.try_transition(JobState.STAGING_OUT_CR, JobState.CANCELLED)
        else:
            job.state = result

    def _process_jobs(self, check_remote):
        """
        Go through the jobs and do what needs to be done.

        Args:
            check_remote (boolean): Whether to access the remote
                compute resource to check on jobs.
        """
        jobs = self._job_store.list_jobs()
        for job_id in [job.id for job in jobs]:
            if self._shutting_down:
                break

            try:
                job = self._job_store.get_job(job_id)
                self._logger.debug('Processing job ' + job_id + ' with current state ' + job.state.value)

                if check_remote and JobState.is_remote(job.state):
                    self._logger.debug('Checking remote state')
                    self._job_runner.update_job(job_id)
                    self._remote_files.update_job(job_id)
                    job = self._job_store.get_job(job_id)

                if job.state == JobState.FINISHED:
                    self._destage_job(job_id, job)

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

    def execute_jobs(self):
        """Run the main backend execution loop.
        """
        with self._job_store:
            last_active = time.perf_counter() - self._remote_refresh - 1
            while not self._shutting_down:
                now = time.perf_counter()
                check_remote = now - last_active > self._remote_refresh

                self._process_jobs(check_remote)
                if check_remote:
                    last_active = time.perf_counter()

                try:
                    # Handler in run_back_end throws KeyboardInterrupt in order to
                    # break the sleep call; catch it to exit gracefully
                    time.sleep(0.1)
                except KeyboardInterrupt:
                    pass

        self._logger.info('Back-end shutting down')
