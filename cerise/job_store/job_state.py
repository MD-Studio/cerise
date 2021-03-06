from enum import Enum


class JobState(Enum):
    """Enum JobState
    """
    # Normal processing
    SUBMITTED = "Submitted"
    STAGING_IN = "StagingIn"
    WAITING = "Waiting"
    RUNNING = "Running"
    FINISHED = "Finished"
    STAGING_OUT = "Destaging"
    SUCCESS = "Success"

    # Cancellation
    STAGING_IN_CR = "StagingCR"
    WAITING_CR = "WaitingCR"
    RUNNING_CR = "RunningCR"
    STAGING_OUT_CR = "DestagingCR"
    CANCELLED = "Cancelled"

    # Error states
    SYSTEM_ERROR = "SystemError"
    TEMPORARY_FAILURE = "TemporaryFailure"
    PERMANENT_FAILURE = "PermanentFailure"

    @staticmethod
    def is_final(state: 'JobState') -> bool:
        """Return whether the JobState is a final state.

        Args:
            state (JobState): The JobState member to analyse.

        Returns:
            bool: True if a job in this state will remain in this
                  state indefinitely.
        """
        return state in [
            JobState.SUCCESS, JobState.CANCELLED, JobState.PERMANENT_FAILURE,
            JobState.TEMPORARY_FAILURE, JobState.SYSTEM_ERROR
        ]

    @staticmethod
    def cancellation_active(state: 'JobState') -> bool:
        """Return whether the JobState indicates that the job has been
        marked for cancellation, but is not cancelled yet.

        These are the _CR states.

        Args:
            state (JobState): The JobState member to analyse.

        Returns:
            bool: True if a job in this state has been marked for
                  cancellation.
        """
        return state in [
            JobState.STAGING_IN_CR, JobState.WAITING_CR, JobState.RUNNING_CR,
            JobState.STAGING_OUT_CR
        ]

    @staticmethod
    def is_remote(state: 'JobState') -> bool:
        """Return whether the state is one in which we expect the
        remote resource to do something to advance it to the next
        state.

        These are WAITING, RUNNING, and the corresponding _CR states.

        Args:
            state (JobState): The JobState member to analyse.

        Returns:
            bool: True iff this state is remote.
        """
        return state in [
            JobState.WAITING, JobState.WAITING_CR, JobState.RUNNING,
            JobState.RUNNING_CR
        ]

    @staticmethod
    def to_cwl_state_string(state: 'JobState') -> str:
        """Return a string containing the CWL state corresponding to
        this state.

        Args:
            state (JobState): The JobState member to convert.

        Returns:
            A string describing the argument as a CWL state.
        """
        state_to_cwl_string = {
            JobState.SUBMITTED: 'Waiting',
            JobState.STAGING_IN: 'Waiting',
            JobState.WAITING: 'Running',
            JobState.RUNNING: 'Running',
            JobState.FINISHED: 'Running',
            JobState.STAGING_OUT: 'Running',
            JobState.SUCCESS: 'Success',
            JobState.STAGING_IN_CR: 'Waiting',
            JobState.WAITING_CR: 'Running',
            JobState.RUNNING_CR: 'Running',
            JobState.STAGING_OUT_CR: 'Running',
            JobState.CANCELLED: 'Cancelled',
            JobState.SYSTEM_ERROR: 'SystemError',
            JobState.TEMPORARY_FAILURE: 'TemporaryFailure',
            JobState.PERMANENT_FAILURE: 'PermanentFailure',
        }
        return state_to_cwl_string[state]
