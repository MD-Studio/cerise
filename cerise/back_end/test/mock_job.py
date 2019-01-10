import logging
from time import time

from cerise.job_store.job_state import JobState


class MockJob:
    """This class provides an in-memory implementation of a job.

    It's used for testing, so that we don't need to bother with a
    database there.
    """

    # Attributes:
    def __init__(self, job_id, name, workflow, job_input):
        """Creates a new Job object.

        The state of a newly created job is JobState.SUBMITTED.

        Args:
            id (str): The id of the job, a string containing a GUID
            name (str): The name of the job, as given by the user
            workflow (str): The URI of the workflow file
            job_input (str): An input definition for the job
        """
        # General description
        self.id = job_id
        """str: Job id, a string containing a UUID."""
        self.name = name
        """str: Name, as specified by the submitter."""
        self.workflow = workflow
        """str: Workflow file URI, as specified by the submitter."""
        self.local_input = job_input
        """str: Input JSON string, as specified by the submitter."""

        # Current status
        self.state = JobState.SUBMITTED
        """JobState: Current state of the job."""
        self.resolve_retry_count = 0
        """int: Number of times we've tried to resolve inputs."""
        self.please_delete = False
        """bool: Whether deletion of the job has been requested."""
        self.log = ''
        """str: Log output as of last update."""
        self.remote_output = ''
        """str: cwl-runner output as of last update."""
        self.remote_error = ''
        """str: cwl-runner stderr output as of last update."""

        # Post-resolving data
        self.workflow_content = None
        """Union[bytes, NoneType]: The content of the workflow
        description file, or None if it has not been resolved yet.
        """
        self.required_num_cores = 0
        """The number of cores to reserve for this workflow.
        If 0, use cluster default."""
        self.time_limit = 0
        """The time to reserve, in seconds.
        If 0, use cluster default."""

        # Post-staging data
        self.remote_workdir_path = ''
        """str: The absolute remote path of the working directory."""
        self.remote_workflow_path = ''
        """str: The absolute remote path of the CWL workflow file."""
        self.remote_input_path = ''
        """str: The absolute remote path of the input description file."""
        self.remote_stdout_path = ''
        """str: The absolute remote path of the standard output dump."""
        self.remote_stderr_path = ''
        """str: The absolute remote path of the standard error dump."""

        # Post-destaging data
        self.local_output = ''
        """str: The serialised JSON output object describing the
        destaged outputs.
        """

        # Internal data
        self.remote_job_id = None
        """str: The id the remote scheduler gave to this job."""

        # Logging
        self.__log = list()
        """list: The job's execution log: [(level, time, msg)]."""

    def try_transition(self, from_state, to_state):
        """Attempts to transition the job's state to a new one.

        If the current state equals from_state, it is set to to_state,
        and True is returned, otherwise False is returned and the
        current state remains what it was.

        Args:
            from_state (JobState): The expected current state
            to_state (JobState): The desired next state

        Returns:
            True iff the transition was successful.
        """
        if self.state == from_state:
            self.state = to_state
            return True
        return False

    def add_log(self, level, message):
        """Add a message to the job's log.

        Args:
            level (logging.LogLevel): Level of importance
            message (str): The log message.
        """
        self.__log.append((level, time(), message))

    def debug(self, message):
        """Add a message to the job's log at level DEBUG.

        Args:
            message (str): The log message.
        """
        self.add_log(logging.DEBUG, message)

    def info(self, message):
        """Add a message to the job's log at level INFO.

        Args:
            message (str): The log message.
        """
        self.add_log(logging.INFO, message)

    def warning(self, message):
        """Add a message to the job's log at level WARNING.

        Args:
            message (str): The log message.
        """
        self.add_log(logging.WARNING, message)

    def error(self, message):
        """Add a message to the job's log at level ERROR.

        Args:
            message (str): The log message.
        """
        self.add_log(logging.ERROR, message)

    def critical(self, message):
        """Add a message to the job's log at level CRITICAL.

        Args:
            message (str): The log message.
        """
        self.add_log(logging.CRITICAL, message)
