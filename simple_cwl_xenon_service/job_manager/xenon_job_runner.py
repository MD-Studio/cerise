import jpype
import requests
import xenon
from xenon.files import OpenOption

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
        self._fs = None
        """The Xenon remote file system to stage to."""
        self._basedir = None
        """The remote path to the base directory where we store our stuff."""

        self._init_xenon(xenon_config)
        self._init_remote(xenon_config)

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
        self._fs = self._x.files().newFileSystem('local', None, None, None)

    def _init_remote(self, xenon_config):
        """Set up a couple of dirs for running jobs, if they don't exist.

        Args:
            xenon_config: A dict containing key-value pairs with Xenon
                configuration.
        """
        # TODO: use config
        self._basedir = '/tmp/simple_cwl_xenon_service'
        basedirpath = self._make_xenon_path('')
        if not self._x.files().exists(basedirpath):
            raise RuntimeError('Configuration error: Base directory not found on remote file system')

        self._make_remote_dir('jobs', True)

        # TODO: do a simple test run to check that cwl-runner works (?)


    def update(self, job_id):
        """Get status from Xenon and update store.

        Args:
            job_id: ID of the job to get the status of.
        """
        job = self._job_store.get_job(job_id)

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
        for job in self._job_store:
            self.update(job.get_id())

    def start_job(self, job_id):
        """Get a job from the job store and start it on the compute resource.

        Args:
            job_id: The id of the job to start.

        Returns:
            None
        """
        job = self._job_store.get_job(job_id)

        # create work dir
        job_dir = 'jobs/' + job_id
        job_workdir = job_dir + '/work'
        self._make_remote_dir(job_workdir)

        # stage workflow
        workflow_file_name = job_dir + '/workflow.cwl'
        if '://' in job.workflow:
            workflow_content = requests.get(job.workflow).content
        else:
            workflow_content = open(job.workflow, 'rb').read()
        self._stage_file(workflow_file_name, workflow_content)

        # stage name of the job
        self._stage_file(job_dir + '/name.txt', job.get_name().encode('utf-8'))

        # submit job
        xenon_jobdesc = xenon.jobs.JobDescription()
        xenon_jobdesc.setWorkingDirectory(self._to_remote_path(job_workdir))
        xenon_jobdesc.setExecutable('cwl-runner')
        xenon_jobdesc.setArguments([self._to_remote_path(workflow_file_name)])
        xenon_jobdesc.setStdout(self._to_remote_path(job_dir + '/stdout.txt'))
        xenon_jobdesc.setStderr(self._to_remote_path(job_dir + '/stderr.txt'))
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
        # delete files from server
        job_dir = 'jobs/' + job_id
        self._rm_remote_dir(job_dir, True)

    def _run_remote(self, rel_workdir, command, args):
        desc = xenon.jobs.JobDescription()
        desc.setWorkingDirectory(self._to_remote_path(rel_workdir))
        desc.setExecutable(command)
        desc.setArguments(args)
        xenon_job = self._x.jobs().submitJob(self._sched, desc)
        # Note: setting to 0 for an infinite timeout doesn't work
        self._x.jobs().waitUntilDone(xenon_job, 100000)

    def _make_remote_dir(self, rel_path, existing_ok=False):
        try:
            xenonpath = self._make_xenon_path(rel_path)
            self._x.files().createDirectories(xenonpath)
        except jpype.JException(xenon.nl.esciencecenter.xenon.files.PathAlreadyExistsException):
            if not existing_ok:
                raise
            else:
                pass

    def _rm_remote_dir(self, rel_path, recursive):
        x_remote_path = self._make_xenon_path(rel_path)
        if recursive:
            self._x_recursive_delete(x_remote_path)
        else:
            self._x.files().delete(x_remote_path)

    def _x_recursive_delete(self, x_remote_path):
        x_dir = self._x.files().newAttributesDirectoryStream(x_remote_path)
        x_dir_it = x_dir.iterator()
        while x_dir_it.hasNext():
            x_path_attr = x_dir_it.next()
            if x_path_attr.attributes().isDirectory():
                self._x_recursive_delete(x_path_attr.path())
            else:
                self._x.files().delete(x_path_attr.path())
        self._x.files().delete(x_remote_path)

    def _stage_file(self, rel_path, data):
        """Write a file on the remote resource containing the given raw data.
        Args:
            rel_path A string containing a relative remote path
            data A bytes-type object containing the data to write
        """
        x_remote_path = self._make_xenon_path(rel_path)
        stream = self._x.files().newOutputStream(x_remote_path, [OpenOption.CREATE, OpenOption.TRUNCATE])
        stream.write(data)
        stream.close()

    def _to_remote_path(self, rel_path):
        return self._basedir + '/' + rel_path

    def _make_xenon_path(self, rel_path):
        remote_path = self._to_remote_path(rel_path)
        xenon_path = xenon.files.RelativePath(remote_path)
        return self._x.files().newPath(self._fs, xenon_path)
