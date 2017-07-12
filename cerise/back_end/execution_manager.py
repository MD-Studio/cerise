from cerise.job_store.sqlite_job_store import SQLiteJobStore
from cerise.job_store.job_state import JobState
from .cwl import get_cwltool_result
from .cwl import is_workflow
from .local_files import LocalFiles
from .xenon_remote_files import XenonRemoteFiles
from .xenon_job_runner import XenonJobRunner

import logging
import time

class ExecutionManager:
    def __init__(self, config, api_config, apidir, xenon):
        self._logger = logging.getLogger(__name__)

        self._shutting_down = False

        # _job_store = InMemoryJobStore()
        self._job_store = SQLiteJobStore(config['database']['file'])
        self._local_files = LocalFiles(self._job_store, config['client-file-exchange'])
        self._remote_files = XenonRemoteFiles(self._job_store, xenon, api_config['compute-resource'])
        self._job_runner = XenonJobRunner(self._job_store, xenon, api_config['compute-resource'])

        self._logger.info('Started back-end')

        self._remote_files.stage_api(apidir)
        # TODO: recover database from crash
        with self._job_store:
            for job in self._job_store.list_jobs():
                if job.state == JobState.STAGING:
                    self._remote_files.delete_job(job.id)
                    job.state = JobState.SUBMITTED
                if job.state == JobState.DESTAGING:
                    self._local_files.delete_output_dir(job.id)
                    job.state = JobState.FINISHED

        # if job is in WAITING, it may never have been started
        # should check somehow whether a job in WAITING actually
        # was submitted to the compute resource

        # for each job in WAITING_CR or RUNNING_CR
            # if it's running
                # send cancel request

    def shutdown(self):
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

        if job.try_transition(JobState.STAGING_CR, JobState.CANCELLED):
            self._logger.debug('Job was cancelled while resolving input')
            return
        self._remote_files.stage_job(job_id, input_files)
        self._job_runner.start_job(job_id)
        if not (job.try_transition(JobState.STAGING, JobState.WAITING) or
                job.try_transition(JobState.STAGING_CR, JobState.WAITING_CR)):
            job.state = JobState.SYSTEM_ERROR

    def _destage_job(self, job_id, job):
        result = get_cwltool_result(job.log)

        if result == JobState.SUCCESS:
            if job.try_transition(JobState.FINISHED, JobState.DESTAGING):
                output_files = self._remote_files.destage_job_output(job_id)
                if output_files is not None:
                    self._local_files.publish_job_output(job_id, output_files)
                    job.try_transition(JobState.DESTAGING, JobState.SUCCESS)
                    job.try_transition(JobState.DESTAGING_CR, JobState.CANCELLED)
        else:
            job.state = result

    def execute_jobs(self):
        """Run the main backend execution loop.
        """
        with self._job_store:
            while not self._shutting_down:
                jobs = self._job_store.list_jobs()
                for job_id in [job.id for job in jobs]:
                    if self._shutting_down:
                        break

                    job = self._job_store.get_job(job_id)
                    self._logger.debug('Processing job ' + job_id + ' with current state ' + job.state.value)

                    if JobState.is_remote(job.state):
                        self._job_runner.update_job(job_id)
                        self._remote_files.update_job(job_id)
                        job = self._job_store.get_job(job_id)

                    if job.state == JobState.FINISHED:
                        self._destage_job(job_id, job)

                    if job.try_transition(JobState.SUBMITTED, JobState.STAGING):
                        self._stage_and_start_job(job_id, job)
                        self._logger.debug('Staged and started job')

                    if JobState.cancellation_active(job.state):
                        self._cancel_job(job_id, job)

                    self._logger.debug('State is now ' + job.state.value)

                    if job.please_delete and JobState.is_final(job.state):
                        self._delete_job(job_id, job)
                self._logger.debug('Sleeping for 2 seconds')
                try:
                    # Handler in run_back_end throws KeyboardInterrupt in order to
                    # break the sleep call; catch it to exit gracefully
                    time.sleep(2)
                except KeyboardInterrupt:
                    pass

        self._logger.info('Back-end shutting down')
