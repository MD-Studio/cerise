from simple_cwl_xenon_service.job_store.sqlite_job_store import SQLiteJobStore
from simple_cwl_xenon_service.job_store.job_state import JobState
from .local_files import LocalFiles
from .xenon_remote_files import XenonRemoteFiles
from .xenon_job_runner import XenonJobRunner

import logging
import time

class ExecutionManager:
    def __init__(self, config, xenon):
        self._logger = logging.getLogger(__name__)

        self._shutting_down = False

        # _job_store = InMemoryJobStore()
        self._job_store = SQLiteJobStore(config['database']['file'])
        self._local_files = LocalFiles(self._job_store, config['client-file-exchange'])
        self._remote_files = XenonRemoteFiles(self._job_store, xenon, config['compute-resource'])
        self._job_runner = XenonJobRunner(self._job_store, xenon, config['compute-resource'])

        self._logger.info('Started back-end')

        # TODO: recover database from crash
        # for each jobs that is in an active state
            # move it back to the corresponding rest state

        # if job is in WAITING, it may never have been started
        # should check somehow whether a job in WAITING actually
        # was submitted to the compute resource

        # for each job in WAITING_CR or RUNNING_CR
            # if it's running
                # send cancel request

    def shutdown(self):
        self._logger.debug('Shutdown requested')
        self._shutting_down = True

    def execute_jobs(self):
        with self._job_store:
            while not self._shutting_down:
                jobs = self._job_store.list_jobs()
                for job_id in [job.id for job in jobs]:
                    if self._shutting_down:
                        break
                    self._job_runner.update_job(job_id)
                    job = self._job_store.get_job(job_id)
                    self._logger.debug('Processing job ' + job_id + ' with current state ' + job.state.value)

                    if job.state == JobState.FINISHED:
                        output_files = self._remote_files.update_job(job_id)
                        if output_files is not None:
                            self._local_files.publish_job_output(job_id, output_files)
                            job.try_transition(JobState.DESTAGING, JobState.SUCCESS)
                            job.try_transition(JobState.DESTAGING_CR, JobState.CANCELLED)
                            # TODO: enable cancellation during destage

                    if job.try_transition(JobState.SUBMITTED, JobState.STAGING):
                        input_files = self._local_files.resolve_input(job_id)
                        if job.try_transition(JobState.STAGING_CR, JobState.CANCELLED):
                            continue
                        self._remote_files.stage_job(job_id, input_files)
                        self._job_runner.start_job(job_id)

                    if JobState.cancellation_active(job.state):
                        self._job_runner.cancel_job(job_id)

                    self._logger.debug('State is now ' + job.state.value)

                    if job.please_delete and JobState.is_final(job.state):
                        self._logger.debug('Deleting job ' + job_id)
                        self._remote_files.delete_job(job_id)
                        if job.state == JobState.SUCCESS:
                            self._local_files.delete_output_dir(job_id)
                        self._job_store.delete_job(job_id)
                self._logger.debug('Sleeping for 2 seconds')
                try:
                    # Handler in run_back_end throws KeyboardInterrupt in order to
                    # break the sleep call; catch it to exit gracefully
                    time.sleep(2)
                except KeyboardInterrupt:
                    pass

        self._logger.info('Back-end shutting down')
