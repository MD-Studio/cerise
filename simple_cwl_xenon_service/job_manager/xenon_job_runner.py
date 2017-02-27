import requests
import xenon
from xenon.files import OpenOption

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
        self._run_remote('', 'mkdir', ['jobs'])

        # TODO: do a simple test run to check that cwl-runner works (?)


    def update(self, job_id):
        """Get status from Xenon and update store.

        Args:
            job_id: ID of the job to get the status of.
        """
        # self.store.get_job(job_id).set_state(JobState.RUNNING)
        return None

    def start_job(self, job_id):
        """Get a job from the job store and start it on the compute resource.

        Args:
            job_id: The id of the job to start.

        Returns:
            None
        """
        job = self._job_store.get_job(job_id)

        # create work dir
        self._run_remote('jobs', 'mkdir', ['-p', job_id + '/work'])
        job_dir = 'jobs/' + job_id
        job_workdir = job_dir + '/work'

        # stage workflow
        workflow_file_name = job_dir + '/workflow.cwl'
        if '://' in job.workflow:
            workflow_content = requests.get(job.workflow).content
        else:
            workflow_content = open(job.workflow, 'rb').read()
        self._stage_file(workflow_file_name, workflow_content)

        # submit job
        xenon_jobdesc = xenon.jobs.JobDescription()
        xenon_jobdesc.setWorkingDirectory(job_workdir)
        xenon_jobdesc.setExecutable('cwl-runner')
        xenon_jobdesc.setArguments([workflow_file_name])
        xenon_jobdesc.setStdout(job_dir + '/stdout.txt')
        xenon_jobdesc.setStderr(job_dir + '/stderr.txt')
        xenon_job = self._x.jobs().submitJob(self._sched, xenon_jobdesc)
        job.set_runner_data(xenon_job)


    def _run_remote(self, rel_workdir, command, args):
        desc = xenon.jobs.JobDescription()
        desc.setWorkingDirectory(self._to_remote_path(rel_workdir))
        desc.setExecutable(command)
        desc.setArguments(args)
        xenon_job = self._x.jobs().submitJob(self._sched, desc)
        # Note: setting to 0 for an infinite timeout doesn't work
        self._x.jobs().waitUntilDone(xenon_job, 100000)

    def _stage_file(self, rel_path, data):
        remote_path = self._to_remote_path(rel_path)
        x_files = self._x.files()
        x_remote_path = x_files.newPath(self._fs, xenon.files.RelativePath(remote_path))
        stream = x_files.newOutputStream(x_remote_path, [OpenOption.CREATE, OpenOption.TRUNCATE])
        stream.write(data)
        stream.close()

    def _to_remote_path(self, rel_path):
        return self._basedir + '/' + rel_path

