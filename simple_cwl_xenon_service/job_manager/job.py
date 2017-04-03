from .job_state import JobState

class Job:
    """This class provides the internal representation of a job. These
    are stored inside the service. Note that there is also a JobDescription,
    which is defined in the Swagger definition and part of the REST API,
    and a Xenon Job class, which represents a job running on the remote
    compute resource.
    """
    # Attributes:
    def __init__(self, id, name, workflow, input):
        """Creates a new Job object.

        The state of a newly created job is JobState.WAITING.

        Args:
            id: The id of the job, a string containing a GUID
            name: A string containing the name of the job, as given by the user
            workflow: A string containing the URI of the workflow file
            input: A string containing an input definition for the job
        """
        # General description
        self.id = id
        """Job id, a string containing a UUID."""
        self.name = name
        """Name, as specified by the submitter."""
        self.workflow = workflow
        """Workflow file URI, as specified by the submitter."""
        self.input = input

        # Current status
        """A string containing the JSON input definition"""
        self.state = JobState.WAITING
        """Current state of the job."""
        self.log = ''
        """String with log output as of last update."""
        self.output = ''
        """String with cwl-runner output as of last update."""

        # Post-resolving data
        self.workflow_content = None
        """A bytes object containing the content of the workflow
        description file, or None if it has not been resolved yet."""

        self.input_files = None
        """A list of (input_name, location, content) of input file contents,
        or None if there is no input yet."""

        # Post-staging data
        self.workdir_path = ''
        """The absolute remote path of the working directory."""
        self.workflow_path = ''
        """The absolute remote path of the CWL workflow file."""
        self.input_path = ''
        """The absolute remote path of the input description file."""
        self.stdout_path = ''
        """The absolute remote path of the standard output dump."""
        self.stderr_path = ''
        """The absolute remote path of the standard error dump."""

        # Post-destaging data
        self.output_files = None
        """A list of (output_name, file_name, content) of output file contents,
        or None if there is no output yet."""

        # Internal data
        self.runner_data = None
        """Unspecified object with data for XenonJobRunner."""
