from .job_state import JobState

class Job:
    """Class Job
    """
    # Attributes:
    def __init__(self, id, name, workflow, input):
        self.id = id
        self.name = name
        self.workflow = workflow
        self.input = input
        self.state = JobState.WAITING

    # Operations
    def get_id(self):
        """Returns the id of the job.

        Return:
            A string containing the job's id.
        """
        return self.id

    def get_name(self):
        """Returns the name of the job.

        Return:
            A string containing the job's id.
        """
        return self.name

    def get_state(self):
        """function get_state

        returns JobState
        """
        return self.state

    def set_state(self, new_state):
        """Update state.

        Args:
            new_state: The new state of the Job.
        """
        self.state = new_state

    def get_output(self):
        """function get_output

        returns string
        """
        return "Here be output"

    def get_log(self):
        """function get_log

        returns string
        """
        return "Here be logging output"

    def cancel(self):
        """function cancel

        returns void
        """
        self.state = JobState.CANCELLED
        return None

    def set_runner_data(self, runner_data):
        self.runner_data = runner_data

    def get_runner_data(self):
        return self.runner_data
