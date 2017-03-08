import jpype
import requests
import xenon
from xenon.files import OpenOption

from .xenon_remote_files import XenonRemoteFiles
from .job_state import JobState

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
        # The try-except ignores an error from Xenon about double initialisation.
        # I'm not doing that as far as I can see, but it seems that PyTest does,
        # because without this, I get that error when trying to run the tests.
        # It still throws on shutdown if you actually use Xenon. TODO
        try:
            xenon.init()
        except ValueError:
            pass
        self._x = xenon.Xenon()
        # TODO: use config
        self._sched = self._x.jobs().newScheduler('local', None, None, None)

    def update(self, job_id):
        """Get status from Xenon and update store.

        Args:
            job_id: ID of the job to get the status of.
        """
        job = self._job_store.get_job(job_id)
        log = self._files.read_from_file(job_id, 'stderr.txt')

        if len(log) > 0:
            job.set_log(log.decode())

        if (   job.get_state() == JobState.WAITING
            or job.get_state() == JobState.RUNNING
           ):
            xenon_job = job.get_runner_data()
            try:
                xenon_status = self._x.jobs().getJobStatus(xenon_job)
            except xenon.exceptions.XenonException:
                # Xenon does not know about this job anymore
                # We should be able to get a status once after the job
                # finishes, so something went wrong
                print('Job disappeared?')
                job.set_state(JobState.SYSTEM_ERROR)
                return

            # convert the xenon JobStatus to our JobStatus
            if xenon_status.isRunning():
                job.set_state(JobState.RUNNING)
                return

            if xenon_status.isDone():
                if xenon_status.hasException():
                    # TODO: fix, check that it is a JobCanceledException
                    print(xenon_status.getException())
                    job.set_state(JobState.CANCELLED)
                    return

                exit_code = xenon_status.getExitCode().intValue()
                if exit_code == 0:
                    job.set_state(JobState.SUCCESS)
                    return
                if exit_code == 1:
                    job.set_state(JobState.PERMANENT_FAILURE)
                    return
                if exit_code == 33:
                    job.set_state(JobState.PERMANENT_FAILURE)
                    return
                job.set_state(JobState.SYSTEM_ERROR)
                return

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
        if '://' in job.workflow:
            workflow_content = requests.get(job.workflow).content
        else:
            workflow_content = open(job.workflow, 'rb').read()
        self._files.write_to_file(job_id, 'workflow.cwl', workflow_content)

        # stage name of the job
        self._files.write_to_file(job_id, 'name.txt', job.get_name().encode('utf-8'))

        # submit job
        xenon_jobdesc = xenon.jobs.JobDescription()
        xenon_jobdesc.setWorkingDirectory(self._files.get_work_dir_path(job_id))
        xenon_jobdesc.setExecutable('cwl-runner')
        xenon_jobdesc.setArguments([self._files.get_remote_file_path(job_id, 'workflow.cwl')])
        xenon_jobdesc.setStdout(self._files.get_remote_file_path(job_id, '/stdout.txt'))
        xenon_jobdesc.setStderr(self._files.get_remote_file_path(job_id, '/stderr.txt'))
        xenon_job = self._x.jobs().submitJob(self._sched, xenon_jobdesc)
        job.set_runner_data(xenon_job)
        job.set_state(JobState.WAITING)

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
