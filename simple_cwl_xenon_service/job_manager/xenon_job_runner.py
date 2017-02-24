import requests
import xenon
from xenon.files import OpenOption

class XenonJobRunner:
    def __init__(self, job_store, xenon_config={}):
        self._job_store = job_store
        self._x = None          # Xenon instance
        self._sched = None      # Xenon scheduler
        self._fs = None         # Xenon remote filesystem
        self._basedir = None    # Remote path to base directory

        self._init_xenon(xenon_config)
        self._init_remote(xenon_config)

    def _init_xenon(self, xenon_config):
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
        # set up a couple of dirs for running jobs, if they don't exist
        # TODO: use config
        self._basedir = '/tmp/simple_cwl_xenon_service'
        self._run_remote('', 'mkdir', ['jobs'])

        # TODO: do a simple test run to check that cwl-runner works (?)


    def update(self):
        # get status from Xenon and update store
        # self.store.get_job(job_id).set_state(JobState.RUNNING)
        return None

    def start_job(self, job_id):
        job = self._job_store.get_job(job_id)

        # create work dir
        self._run_remote('jobs', 'mkdir', ['-p', job_id + '/work'])
        job_dir = 'jobs/' + job_id
        job_workdir = job_dir + '/work'

        # stage workflow
        workflow_file_name = job_dir + '/workflow.cwl'
        workflow_content = requests.get(job.workflow).content
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

