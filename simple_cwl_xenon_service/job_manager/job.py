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
        self._id = id
        self._name = name
        self._workflow = workflow
        self._input = input
        self._state = JobState.WAITING
        self._log = ''
        self._output = ''

    # Operations
    def get_id(self):
        """Returns the id of the job.

        Returns:
            A string containing the job's id.
        """
        return self._id

    def get_name(self):
        """Returns the name of the job.

        Returns:
            A string containing the job's name.
        """
        return self._name

    def get_workflow(self):
        """Returns the workflow URI.

        Returns:
            A string containing a URI pointing to the workflow.
        """
        return self._workflow

    def get_input(self):
        """Returns the input description.

        Returns:
            A string containing the CWL input definition of the job.
        """
        return self._input

    def get_state(self):
        """Returns the current state of the job.

        Returns:
            A string from JobState.*
        """
        return self._state

    def set_state(self, new_state):
        """Set the current state of the job.

        Args:
            new_state: The new state of the Job, one of JobState.*
        """
        self._state = new_state

    def get_output(self):
        """Returns the output of the job

        Returns:
            The output of the job run
        """
        return self._output

    def set_output(self, output):
        """Sets the output contents of the job

        Args:
            output: A string containing a JSON CWL output object
        """
        self._output = output

    def get_log(self):
        """Returns the log of the job

        Returns:
            The run log of the job
        """
        return self._log

    def set_log(self, log):
        """Set the log contents of the job

        Args:
            log: A string containing the log output of the job
        """
        self._log = log

    def set_runner_data(self, runner_data):
        """Set runner data.

        This is a way for the XenonJobRunner to attach data to a job in
        the job store.

        Args:
            runner_data: Some object to attach
        """

        self._runner_data = runner_data

    def get_runner_data(self):
        """Get runner data.

        This is a way for the XenonJobRunner to attach data to a job in
        the job store. This function retrieves it again.

        Returns:
            The object that was attached in a previous call to
            set_runner_data()
        """
        return self._runner_data
