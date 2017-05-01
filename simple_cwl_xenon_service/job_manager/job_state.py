from enum import Enum
class JobState(Enum):
    """Enum JobState
    """
    WAITING = "Waiting"
    RUNNING = "Running"
    SUCCESS = "Success"
    CANCELLED = "Cancelled"
    SYSTEM_ERROR = "SystemError"
    TEMPORARY_FAILURE = "TemporaryFailure"
    PERMANENT_FAILURE = "PermanentFailure"

    FINISHED = "Finished"   # temporary hack

    @staticmethod
    def is_cancellable(state):
        """Return whether the JobState is one in which a job can be
        cancelled.

        A job can be cancelled when it is waiting or running.

        Args:
            state (JobState): The JobState member to analyse.

        Returns:
            bool: True if a job in this state can be cancelled, False otherwise
        """
        return (state == JobState.WAITING) or (state == JobState.RUNNING)

    @staticmethod
    def is_done(state):
        """Return whether the JobState is one in which the job is done.

        Args:
            state (JobState): The JobState member to analyse.

        Returns:
            bool: True If a job in this state is not running, and is not
            waiting to run at some point in the future.
        """
        return (state != JobState.WAITING) and (state != JobState.RUNNING)

    @staticmethod
    def to_external_string(state):
        """Return a string describing this JobState.

        Args:
            state (JobState): The JobState member to convert.

        Returns:
            A string describing the argument.
        """
        state_to_string = {
            JobState.WAITING: 'Waiting',
            JobState.RUNNING: 'Running',
            JobState.SUCCESS: 'Success',
            JobState.CANCELLED: 'Cancelled',
            JobState.SYSTEM_ERROR: 'SystemError',
            JobState.TEMPORARY_FAILURE: 'TemporaryFailure',
            JobState.PERMANENT_FAILURE: 'PermanentFailure',
        }
        return state_to_string[state]
