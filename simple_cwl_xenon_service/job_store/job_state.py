from enum import Enum

class JobState(Enum):
    """Enum JobState
    """
    # Normal processing
    SUBMITTED = "Submitted"
    STAGING = "Staging"
    WAITING = "Waiting"
    RUNNING = "Running"
    FINISHED = "Finished"
    DESTAGING = "Destaging"
    SUCCESS = "Success"

    # Cancellation
    STAGING_CR = "StagingCR"
    WAITING_CR = "WaitingCR"
    RUNNING_CR = "RunningCR"
    DESTAGING_CR = "DestagingCR"
    CANCELLED = "Cancelled"

    # Error states
    SYSTEM_ERROR = "SystemError"
    TEMPORARY_FAILURE = "TemporaryFailure"
    PERMANENT_FAILURE = "PermanentFailure"

    @staticmethod
    def is_final(state):
        """Return whether the JobState is a final state.

        Args:
            state (JobState): The JobState member to analyse.

        Returns:
            bool: True if a job in this state will remain in this
                  state indefinitely.
        """
        return state in [JobState.SUCCESS,
                         JobState.CANCELLED,
                         JobState.PERMANENT_FAILURE,
                         JobState.TEMPORARY_FAILURE,
                         JobState.SYSTEM_ERROR]

    @staticmethod
    def cancellation_active(state):
        """Return whether the JobState indicates that the job has been
        marked for cancellation, but is not cancelled yet.

        These are the _CR states.

        Args:
            state (JobState): The JobState member to analyse.

        Returns:
            bool: True if a job in this state has been marked for
                  cancellation.
        """
        return state in [JobState.STAGING_CR,
                         JobState.WAITING_CR,
                         JobState.RUNNING_CR,
                         JobState.DESTAGING_CR]

    @staticmethod
    def is_remote(state):
        """Return whether the state is one in which we expect the
        remote resource to do something to advance it to the next
        state.

        These are WAITING, RUNNING, and the corresponding _CR states.

        Args:
            state (JobState): The JobState member to analyse.

        Returns:
            bool: True iff this state is remote.
        """
        return state in [JobState.WAITING,
                         JobState.WAITING_CR,
                         JobState.RUNNING,
                         JobState.RUNNING_CR]

    @staticmethod
    def to_cwl_state_string(state):
        """Return a string containing the CWL state corresponding to
        this state.

        Args:
            state (JobState): The JobState member to convert.

        Returns:
            A string describing the argument as a CWL state.
        """
        state_to_cwl_string = {
            JobState.SUBMITTED: 'Waiting',
            JobState.STAGING: 'Waiting',
            JobState.WAITING: 'Waiting',
            JobState.RUNNING: 'Running',
            JobState.FINISHED: 'Running',
            JobState.DESTAGING: 'Running',
            JobState.SUCCESS: 'Success',

            JobState.STAGING_CR: 'Waiting',
            JobState.WAITING_CR: 'Waiting',
            JobState.RUNNING_CR: 'Running',
            JobState.DESTAGING_CR: 'Running',
            JobState.CANCELLED: 'Cancelled',

            JobState.SYSTEM_ERROR: 'SystemError',
            JobState.TEMPORARY_FAILURE: 'TemporaryFailure',
            JobState.PERMANENT_FAILURE: 'PermanentFailure',
        }
        return state_to_cwl_string[state]
