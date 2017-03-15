import jpype
import requests
import xenon
from xenon.files import OpenOption

from .xenon_remote_files import XenonRemoteFiles
from .job_state import JobState

from time import sleep

class XenonJobRunner:
    def __init__(self, job_store, xenon_config={}):
        """Create a XenonJobRunner object.

        Args:
            job_store: The job store to get jobs from.
            xenon_config: A dict containing key-value pairs with Xenon
                configuration.
        """
        self._job_store = job_store
        """The JobStore to obtain jobs from."""
        self._x = None
        """The Xenon instance to use."""
        self._sched = None
        """The Xenon scheduler to start jobs through."""

        self._init_xenon(xenon_config)

        self._files = XenonRemoteFiles(self._x, xenon_config)
        """The Xenon remote file system to stage to."""

    def _init_xenon(self, xenon_config):
        """Initialise Xenon, and set up Xenon objects to work with.

        Args:
            xenon_config: A dict containing key-value pairs with Xenon
                configuration.
        """
        self._x = xenon.Xenon()
        # TODO: use config
        self._sched = self._x.jobs().newScheduler('local', None, None, None)

    def update(self, job_id):
        """Get status from Xenon and update store.

        Args:
            job_id: ID of the job to get the status of.
        """
        job = self._job_store.get_job(job_id)

        # get state
        if (   job.get_state() == JobState.WAITING
            or job.get_state() == JobState.RUNNING
           ):
            try:
                xenon_job = job.get_runner_data()
                xenon_status = self._x.jobs().getJobStatus(xenon_job)
                job.set_state(self._xenon_status_to_job_state(xenon_status))
            except xenon.exceptions.XenonException:
                # Xenon does not know about this job anymore
                # We should be able to get a status once after the job
                # finishes, so something went wrong
                print('Job disappeared?')
                job.set_state(JobState.SYSTEM_ERROR)
                pass

        # get output
        output = self._files.read_from_file(job_id, 'stdout.txt')
        if len(output) > 0:
            job.set_output(output.decode())

        # get log
        log = self._files.read_from_file(job_id, 'stderr.txt')
        if len(log) > 0:
            job.set_log(log.decode())

    def update_all(self):
        """Get status from Xenon and update store, for all jobs.
        """
        for job in self._job_store.list_jobs():
            self.update(job.get_id())

    def start_job(self, job_id):
        """Get a job from the job store and start it on the compute resource.

        Args:
            job_id: The id of the job to start.

        Returns:
            None
        """
        job = self._job_store.get_job(job_id)

        self._files.create_work_dir(job_id)

        # stage workflow
        if '://' in job.get_workflow():
            workflow_content = requests.get(job.get_workflow()).content
        else:
            workflow_content = open(job.get_workflow(), 'rb').read()
        self._files.write_to_file(job_id, 'workflow.cwl', workflow_content)

        # stage input
        self._files.write_to_file(job_id, 'input.json', job.get_input().encode('utf-8'))

        # stage name of the job
        self._files.write_to_file(job_id, 'name.txt', job.get_name().encode('utf-8'))

        # submit job
        xenon_jobdesc = xenon.jobs.JobDescription()
        xenon_jobdesc.setWorkingDirectory(self._files.get_work_dir_path(job_id))
        xenon_jobdesc.setExecutable('cwl-runner')
        args = [
            self._files.get_remote_file_path(job_id, 'workflow.cwl'),
            self._files.get_remote_file_path(job_id, 'input.json')
            ]
        xenon_jobdesc.setArguments(args)
        xenon_jobdesc.setStdout(self._files.get_remote_file_path(job_id, '/stdout.txt'))
        xenon_jobdesc.setStderr(self._files.get_remote_file_path(job_id, '/stderr.txt'))
        xenon_job = self._x.jobs().submitJob(self._sched, xenon_jobdesc)
        job.set_runner_data(xenon_job)
        job.set_state(JobState.WAITING)
        sleep(2)    # work-around for Xenon local running bug

    def cancel_job(self, job_id):
        job = self._job_store.get_job(job_id)
        if JobState.is_cancellable(job.get_state()):
            xenon_job = job.get_runner_data()
            new_status = self._x.jobs().cancelJob(xenon_job)
            job.set_state(JobState.CANCELLED)

    def delete_job(self, job_id):
        job = self._job_store.get_job(job_id)
        self.cancel_job(job_id)
        self._files.remove_work_dir(job_id)

    def _xenon_status_to_job_state(self, xenon_status):
        """Convert a xenon JobStatus to our JobState.

        Args:
            xenon_status: a xenon JobStatus object.

        Returns:
            A corresponding JobState object.
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

