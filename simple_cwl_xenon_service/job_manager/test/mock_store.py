from .context import simple_cwl_xenon_service

from simple_cwl_xenon_service.job_manager.job import Job

class MockStore:
    """Created this by hand rather than using unittest.mock,
    because the work is all in the example data, and that
    can't be automated anyway.
    """

    def __init__(self, test_config):
        self._local_base_path = test_config.get('local-base-path')
        self._remote_base_path = test_config.get('remote-base-path')
        self._jobs = []

    def add_test_job(self, test_job_id, test_job_type, test_job_stage):
        """Create a mock job store containing a single job
        with the given id, type and stage.

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
        # Create
        job = Job(job_id, job_id, "input/pass_workflow.cwl", "{}")
        if stage == "submitted":
            return job

        # Resolve
        job.workflow_content = (
            "#!/usr/bin/env cwl-runner\n"
            "cwlVersion: v1.0\n"
            "class: CommandLineTool\n"
            "baseCommand: echo\n"
            "inputs: []\n"
            "outputs: []\n")

        job.input_files = []

        if stage == "resolved":
            return job

        # Stage
        job.workdir_path = ""

        if stage == "staged":
            return job

        # Destage
        job.output_files = []

        if stage == "destaged":
            return job

        return None

    def _create_wc_job(self, job_id, stage):
        # Create
        job = Job(job_id, job_id, "input/wc_workflow.cwl",
                '{ "file": { "class": "File", "location": "input/hello_world.txt" } }')

        if stage == "submitted":
            return job

        # Resolve
        job.workflow_content = bytes(
                "#!/usr/bin/env cwl-runner\n"
                "\n"
                "cwlVersion: v1.0\n"
                "class: CommandLineTool\n"
                "baseCommand: wc\n"
                "stdout: output.txt\n"
                "inputs:\n"
                "  file:\n"
                "    type: File\n"
                "    inputBinding:\n"
                "      position: 1\n"
                "\n"
                "outputs:\n"
                "  output:\n"
                "    type: File\n"
                "    outputBinding: { glob: output.txt }\n", 'utf-8')

        job.input_files = [('file', 'input/hello_world.txt', bytes(
            'Hello, World!\n'
            '\n'
            'Here is a test file for the staging test.\n'
            '\n', 'utf-8'))]

        if stage == "resolved":
            return job

        # Destage
        job.output = '{ "output": { "class": "File", "location": "output.txt" } }'

        job.output_files = [
                ('output', 'output.txt', bytes(' 4 11 58 hello_world.txt', 'utf-8'))
                ]

        if stage == "destaged":
            return job

        return None

    def _create_missing_input_job(self, job_id, stage):
        # Create
        job = Job(job_id, job_id, "input/wc_workflow.cwl",
                '{ "file": { "class": "File", "location": "input/non_existing_file.txt" } }')

        if stage == "submitted":
            return job;

        return None
