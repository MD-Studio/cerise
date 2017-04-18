from .context import simple_cwl_xenon_service

from .fixture_jobs import PassJob
from .fixture_jobs import WcJob
from .fixture_jobs import MissingInputJob

from simple_cwl_xenon_service.job_manager.job import Job

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
        self._remote_base_path = test_config.get('remote-base-path')
        self._jobs = []

    def add_test_job(self, test_job_id, test_job_type, test_job_stage):
        """Add a mock job with the given id, type and stage.

        Args:
            test_job_id A str containing a unique identifier
            test_job_type The type of test job. See source.
            test_job_stage A str : "submitted", "resolved", "staged",
            "run", "destaged", "done"
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

    def list_jobs(self):
        return self._jobs

    def get_job(self, job_id):
        return [job for job in self._jobs if job.id == job_id][0]

    def delete_job(self, job_id):
        pass

    def _create_pass_job(self, job_id, stage):
        job = Job(job_id, job_id, "input/pass_workflow.cwl", "{}")

        if stage == 'submitted':
            pass_wf_path = os.path.join(self._local_base_path, 'input', 'pass_workflow.cwl')

            with open(pass_wf_path, 'wb') as f:
                f.write(PassJob.workflow)

            return job

        if stage == "resolved":
            job.workflow_content = PassJob.workflow
            job.input_files = []
            return job

        if stage == "staged":
            job.workdir_path = ""
            return job

        if stage == "destaged":
            job.output_files = []
            return job

        raise ValueError('Invalid stage in _create_pass_job')

    def _create_wc_job(self, job_id, stage):
        # Create
        job = Job(job_id, job_id, "input/wc_workflow.cwl", WcJob.input)

        if stage == 'submitted':
            wc_wf_path = os.path.join(self._local_base_path, 'input', 'wc_workflow.cwl')

            with open(wc_wf_path, 'wb') as f:
                f.write(WcJob.workflow)

            for (name, filename, contents) in WcJob.input_files:
                wc_input_path = os.path.join(self._local_base_path, filename)
                with open(wc_input_path, 'wb') as f:
                    f.write(contents)

            return job

        if stage == 'resolved':
            job.workflow_content = WcJob.workflow
            job.input_files = WcJob.input_files
            return job

        if stage == 'destaged':
            job.output = WcJob.output
            job.output_files = WcJob.output_files
            return job

        return ValueError('Invalid stage in _create_wc_job')

    def _create_missing_input_job(self, job_id, stage):
        # Create
        job = Job(job_id, job_id, "input/wc_workflow.cwl", MissingInputJob.input)

        if stage == 'submitted':
            wc_wf_path = os.path.join(self._local_base_path, 'input', 'wc_workflow.cwl')

            with open(wc_wf_path, 'wb') as f:
                f.write(WcJob.workflow)

            return job;

        return ValueError('Invalid stage in _create_missing_input_job')
