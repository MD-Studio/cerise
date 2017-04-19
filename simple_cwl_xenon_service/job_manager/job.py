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
            id (str): The id of the job, a string containing a GUID
            name (str): The name of the job, as given by the user
            workflow (str): The URI of the workflow file
            input (str): An input definition for the job
        """
        # General description
        self.id = id
        """str: Job id, a string containing a UUID."""
        self.name = name
        """str: Name, as specified by the submitter."""
        self.workflow = workflow
        """str: Workflow file URI, as specified by the submitter."""
        self.input = input
        """str: Input JSON string, as specified by the submitter."""

        # Current status
        self.state = JobState.WAITING
        """JobState: Current state of the job."""
        self.log = ''
        """str: Log output as of last update."""
        self.output = ''
        """str: cwl-runner output as of last update."""

        # Post-resolving data
        self.workflow_content = None
        """Union[bytes, NoneType]: The content of the workflow
        description file, or None if it has not been resolved yet.
        """

        self.input_files = None
        """Union[List[Tuple[str, str, bytes]], NoneType]: A list of
        (input_name, location, content) of input file contents,
        or None if there is no input yet."""

        # Post-staging data
        self.workdir_path = ''
        """str: The absolute remote path of the working directory."""
        self.workflow_path = ''
        """str: The absolute remote path of the CWL workflow file."""
        self.input_path = ''
        """str: The absolute remote path of the input description file."""
        self.stdout_path = ''
        """str: The absolute remote path of the standard output dump."""
        self.stderr_path = ''
        """str: The absolute remote path of the standard error dump."""

        # Post-destaging data
        self.output_files = None
        """Union[List[str, str, bytes], NoneType]: A list of
        (output_name, file_name, content) of output file contents,
        or None if there is no output yet."""

        # Internal data
        self.runner_data = None
        """Any: Unspecified object with data for XenonJobRunner."""
