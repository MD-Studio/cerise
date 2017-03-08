
class JobState:
    """Enum JobState
    """
    WAITING = "Waiting"
    RUNNING = "Running"
    SUCCESS = "Success"
    CANCELLED = "Cancelled"
    SYSTEM_ERROR = "SystemError"
    TEMPORARY_FAILURE = "TemporaryFailure"
    PERMANENT_FAILURE = "PermanentFailure"

    @staticmethod
    def is_cancellable(state):
        return (state == JobState.WAITING) or (state == JobState.RUNNING)
