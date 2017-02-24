import xenon

class XenonJobRunner:
    def __init__(self, job_store, xenon_config={}):
        self._init_xenon(xenon_config)
        self._init_remote(xenon_config)
        self._job_store = job_store

    def update(self):
        # get status from Xenon and update store
        # self.store.get_job(job_id).set_state(JobState.RUNNING)
        return None

    def start_job(self, job_id):
        job = self._job_store.get_job(job_id)

        self._run_remote(self.xenon_jobsdir, 'mkdir', ['-p', job_id + '/work'])
        job_dir = self.xenon_jobsdir + '/' + job_id
        job_workdir = job_dir + '/work'

        xenon_jobdesc = xenon.jobs.JobDescription()
        xenon_jobdesc.setWorkingDirectory(job_workdir)
        xenon_jobdesc.setExecutable('cwlrunner')
        xenon_jobdesc.setArguments([job.workflow])
        xenon_jobdesc.setStdout(job_dir + '/stdout.txt')
        xenon_jobdesc.setStderr(job_dir + '/stderr.txt')
        xenon_job = self.x.jobs().submitJob(self.sched, xenon_jobdesc)
        job.set_runner_data(xenon_job)

    def _init_xenon(self, xenon_config):
        # The try-except ignores an error from Xenon about double initialisation.
        # I'm not doing that as far as I can see, but it seems that PyTest does,
        # because without this, I get that error when trying to run the tests.
        try:
            xenon.init()
        except ValueError:
            pass
        self.x = xenon.Xenon()
        # TODO: use config
        self.sched = self.x.jobs().newScheduler('local', None, None, None)

    def _init_remote(self, xenon_config):
        # set up a couple of dirs for running jobs, if they don't exist
        # TODO: use config
        xenon_basedir = '/tmp/simple_cwl_xenon_service'
        self._run_remote(xenon_basedir, 'mkdir', ['jobs'])
        self.xenon_jobsdir = xenon_basedir + '/jobs'

        # do a simple test run to check that cwlrunner works (?)


    def _run_remote(self, workdir, command, args):
        desc = xenon.jobs.JobDescription()
        desc.setWorkingDirectory(workdir)
        desc.setExecutable(command)
        desc.setArguments(args)
        xenon_job = self.x.jobs().submitJob(self.sched, desc)
        self.x.jobs().waitUntilDone(xenon_job, 1000)

