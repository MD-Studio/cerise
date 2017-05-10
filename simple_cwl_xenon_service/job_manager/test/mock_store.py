from .fixture_jobs import PassJob
from .fixture_jobs import WcJob
from .fixture_jobs import MissingInputJob
from .fixture_jobs import SlowJob
from .fixture_jobs import BrokenJob

from simple_cwl_xenon_service.job_manager.in_memory_job import InMemoryJob
from simple_cwl_xenon_service.job_manager.job_state import JobState

import os

class MockStore:
    """Created this by hand rather than using unittest.mock,
    because the work is all in the example data, and that
    can't be automated anyway.
    """

    def __init__(self, test_config):
        """Create a mock job store.

        Args:
            test_config A dict with keys 'local-base-path' and
                'remote-base-path', which are str objects containing
                paths (on the local machine) where local and remote
                files are put for testing purposes.
        """
        self._local_base_path = test_config.get('local-base-path')
        self._local_base_url = 'file://' + self._local_base_path
        self._remote_base_path = test_config.get('remote-base-path')
        self._jobs = []

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def add_test_job(self, test_job_id, test_job_type, test_job_stage):
        """Add a mock job with the given id, type and stage.

        Args:
            test_job_id A str containing a unique identifier
            test_job_type The type of test job. See source.
            test_job_stage A str : "submitted", "resolved", "staged",
            "run", "run_and_updated", "destaged", "done"
        """
        if test_job_type == "pass":
            self._jobs.append(self._create_pass_job(test_job_id, test_job_stage))
        elif test_job_type == "slow":
            self._jobs.append(self._create_slow_job(test_job_id, test_job_stage))
        elif test_job_type == "broken":
            self._jobs.append(self._create_broken_job(test_job_id, test_job_stage))
        elif test_job_type == "missing_input":
            self._jobs.append(self._create_missing_input_job(test_job_id, test_job_stage))
        elif test_job_type == "echo":
            self._jobs.append(self._create_echo_job(test_job_id, test_job_stage))
        elif test_job_type == "wc":
            self._jobs.append(self._create_wc_job(test_job_id, test_job_stage))
        elif test_job_type == "complex":
            self._jobs.append(self._create_complex_job(test_job_id, test_job_stage))

    def get_input_files(self, test_job_type):
        if test_job_type == "pass":
            return []
        elif test_job_type == "broken":
            return []
        elif test_job_type == "wc":
            return WcJob.local_input_files
        raise NotImplementedError

    def get_output_files(self, test_job_type):
        if test_job_type == "pass":
            return None
        elif test_job_type == "wc":
            return WcJob.output_files
        raise NotImplementedError

    def list_jobs(self):
        return self._jobs

    def get_job(self, job_id):
        return [job for job in self._jobs if job.id == job_id][0]

    def delete_job(self, job_id):
        pass

    def _create_pass_job(self, job_id, stage):
        pass_wf_path = os.path.join(self._local_base_path, 'input', 'pass_workflow.cwl')
        job = InMemoryJob(job_id, job_id, 'file://' + pass_wf_path, "{}")

        if stage == 'submitted':
            with open(pass_wf_path, 'wb') as f:
                f.write(PassJob.workflow)

            return job

        if stage == "resolved":
            job.workflow_content = PassJob.workflow
            job.state = JobState.STAGING
            return job

        if stage == "staged":
            pass_jobdir = os.path.join(self._remote_base_path, 'jobs', job_id)
            job.remote_workdir_path = os.path.join(pass_jobdir, 'work')
            job.remote_workflow_path = os.path.join(pass_jobdir,'workflow.cwl')
            job.remote_input_path = os.path.join(pass_jobdir, 'input.json')
            job.remote_stdout_path = os.path.join(pass_jobdir, 'stdout.txt')
            job.remote_stderr_path = os.path.join(pass_jobdir, 'stderr.txt')
            job.state = JobState.STAGING

            os.makedirs(job.remote_workdir_path)

            with open(job.remote_workflow_path, 'wb') as f:
                f.write(PassJob.workflow)

            with open(job.remote_input_path, 'wb') as f:
                f.write(PassJob.remote_input.encode('utf-8'))

            return job

        if stage == 'run' or stage == 'run_and_updated':
            pass_job_dir = os.path.join(self._remote_base_path, 'jobs', job_id, 'work')
            pass_output_dir = os.path.join(pass_job_dir, 'work')
            os.makedirs(pass_output_dir)

            with open(os.path.join(pass_job_dir, 'stdout.txt'), 'wb') as f:
                f.write(PassJob.remote_output.encode('utf-8'))
            job.state = JobState.FINISHED

            if stage == 'run_and_updated':
                job.remote_output = PassJob.remote_output
                job.state = JobState.DESTAGING
            return job

        if stage == "destaged":
            job.remote_output = PassJob.remote_output
            job.local_output = PassJob.local_output
            job.state = JobState.DESTAGING
            return job

        raise ValueError('Invalid stage in _create_pass_job')

    def _create_wc_job(self, job_id, stage):
        # Create
        wc_wf_path = os.path.join(self._local_base_path, 'input', 'wc_workflow.cwl')
        job = InMemoryJob(job_id, job_id, 'file://' + wc_wf_path, WcJob.local_input(self._local_base_url))

        if stage == 'submitted':
            with open(wc_wf_path, 'wb') as f:
                f.write(WcJob.workflow)

            for (_, filename, contents) in WcJob.local_input_files:
                wc_input_path = os.path.join(self._local_base_path, filename)
                with open(wc_input_path, 'wb') as f:
                    f.write(contents)

            return job

        if stage == 'resolved':
            job.workflow_content = WcJob.workflow
            job.state = JobState.STAGING
            return job

        if stage == 'staged':
            wc_jobdir = os.path.join(self._remote_base_path, 'jobs', job_id)
            wc_workdir = os.path.join(wc_jobdir, 'work')
            job.remote_workdir_path = wc_workdir
            job.remote_workflow_path = os.path.join(wc_jobdir,'workflow.cwl')
            job.remote_input_path = os.path.join(wc_jobdir, 'input.json')
            job.remote_stdout_path = os.path.join(wc_jobdir, 'stdout.txt')
            job.remote_stderr_path = os.path.join(wc_jobdir, 'stderr.txt')
            job.state = JobState.STAGING

            os.makedirs(wc_workdir)

            with open(job.remote_workflow_path, 'wb') as f:
                f.write(WcJob.workflow)

            with open(job.remote_input_path, 'wb') as f:
                f.write(WcJob.remote_input.encode('utf-8'))

            for (_, filename, contents) in WcJob.remote_input_files:
                wc_input_path = os.path.join(wc_workdir, filename)
                with open(wc_input_path, 'wb') as f:
                    f.write(contents)

            return job

        if stage == 'run' or stage == 'run_and_updated':
            wc_job_dir = os.path.join(self._remote_base_path, 'jobs', job_id)
            wc_output_dir = os.path.join(wc_job_dir, 'work')
            os.makedirs(wc_output_dir)
            for (name, filename, contents) in WcJob.output_files:
                wc_output_path = os.path.join(wc_output_dir, filename)
                with open(wc_output_path, 'wb') as f:
                    f.write(contents)

            with open(os.path.join(wc_job_dir, 'stdout.txt'), 'wb') as f:
                f.write(WcJob.remote_output('file://' + wc_output_dir).encode('utf-8'))
            job.state = JobState.FINISHED

            if stage == 'run_and_updated':
                job.remote_output = WcJob.remote_output('file://' + wc_output_dir)
                job.state = JobState.DESTAGING
            return job

        if stage == 'destaged':
            job.remote_output = WcJob.remote_output('')
            job.local_output = WcJob.local_output
            job.state = JobState.DESTAGING
            return job

        return ValueError('Invalid stage in _create_wc_job')

    def _create_missing_input_job(self, job_id, stage):
        # Create
        wc_wf_path = os.path.join(self._local_base_path, 'input', 'wc_workflow.cwl')
        job = InMemoryJob(job_id, job_id, 'file://' + wc_wf_path, MissingInputJob.local_input(self._local_base_url))

        if stage == 'submitted':
            with open(wc_wf_path, 'wb') as f:
                f.write(WcJob.workflow)

            return job;

        return ValueError('Invalid stage in _create_missing_input_job')

    def _create_slow_job(self, job_id, stage):
        job = InMemoryJob(job_id, job_id, "input/slow_workflow.cwl", "{}")

        if stage == "staged":
            slow_jobdir = os.path.join(self._remote_base_path, 'jobs', job_id)
            job.remote_workdir_path = os.path.join(slow_jobdir, 'work')
            job.remote_workflow_path = os.path.join(slow_jobdir,'workflow.cwl')
            job.remote_input_path = os.path.join(slow_jobdir, 'input.json')
            job.remote_stdout_path = os.path.join(slow_jobdir, 'stdout.txt')
            job.remote_stderr_path = os.path.join(slow_jobdir, 'stderr.txt')
            job.state = JobState.STAGING

            os.makedirs(job.remote_workdir_path)

            with open(job.remote_workflow_path, 'wb') as f:
                f.write(SlowJob.workflow)

            with open(job.remote_input_path, 'wb') as f:
                f.write(SlowJob.remote_input.encode('utf-8'))

            return job

        raise ValueError('Invalid stage in _create_slow_job')

    def _create_broken_job(self, job_id, stage):
        job = InMemoryJob(job_id, job_id, "input/broken_workflow.cwl", "{}")

        if stage == 'submitted':
            pass_wf_path = os.path.join(self._local_base_path, 'input', 'broken_workflow.cwl')

            with open(pass_wf_path, 'wb') as f:
                f.write(BrokenJob.workflow)

            return job

        if stage == "resolved":
            job.workflow_content = BrokenJob.workflow
            job.state = JobState.STAGING
            return job

        if stage == "staged":
            broken_jobdir = os.path.join(self._remote_base_path, 'jobs', job_id)
            job.remote_workdir_path = os.path.join(broken_jobdir, 'work')
            job.remote_workflow_path = os.path.join(broken_jobdir,'workflow.cwl')
            job.remote_input_path = os.path.join(broken_jobdir, 'input.json')
            job.remote_stdout_path = os.path.join(broken_jobdir, 'stdout.txt')
            job.remote_stderr_path = os.path.join(broken_jobdir, 'stderr.txt')
            job.state = JobState.STAGING

            os.makedirs(job.remote_workdir_path)

            with open(job.remote_workflow_path, 'wb') as f:
                f.write(BrokenJob.workflow)

            with open(job.remote_input_path, 'wb') as f:
                f.write(BrokenJob.remote_input.encode('utf-8'))

            return job

        if stage == 'run' or stage == 'run_and_updated':
            broken_job_dir = os.path.join(self._remote_base_path, 'jobs', job_id, 'work')
            broken_output_dir = os.path.join(broken_job_dir, 'work')
            os.makedirs(broken_output_dir)

            with open(os.path.join(broken_job_dir, 'stdout.txt'), 'wb') as f:
                f.write(BrokenJob.output.encode('utf-8'))

            job.state = JobState.FINISHED

            if stage == 'run_and_updated':
                job.remote_output = BrokenJob.remote_output
                job.state = JobState.DESTAGING

            return job


