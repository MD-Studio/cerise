import logging
from time import asctime, localtime, time
from typing import Any, List, Optional, Union, cast

from cerise.job_store.job_state import JobState


class SQLiteJob:
    """This class provides the internal representation of a job. These
    are stored inside the service. Note that there is also a JobDescription,
    which is defined in the Swagger definition and part of the REST API,
    and a Cerulean JobDescription class, which describes a job to start on
    a remote compute resource.
    """

    def __init__(self, store: Any, job_id: str) -> None:
        """Creates a new SQLiteJob object.

        This contains only a job id and a reference to the store; the
        data about the job are in the database.

        Args:
            store (SQLiteJobStore): The store this job is stored by
            id: The id of the job, a string containing a GUID
        """
        self._store = store
        """SQLiteJobStore: A reference to the store this job is in."""

        self.id = job_id
        """str: Job id, a string containing a UUID."""

    # General description
    @property
    def name(self) -> str:
        """Name, as specified by the submitter.
        """
        return cast(str, self._get_var('name'))

    @property
    def workflow(self) -> str:
        """Workflow file URI, as specified by the submitter.
        """
        return cast(str, self._get_var('workflow'))

    @property
    def local_input(self) -> str:
        """Input JSON string, as specified by the submitter.
        """
        return cast(str, self._get_var('local_input'))

    # Current status
    @property
    def state(self) -> JobState:
        """Current state of the job.
        """
        state_str = cast(str, self._get_var('state'))
        return JobState[state_str]

    @state.setter
    def state(self, value: JobState) -> None:
        self._set_var('state', value.name)

    @property
    def resolve_retry_count(self) -> int:
        """How many times we've tried to resolve.
        """
        return int(self._get_var('resolve_retry_count'))

    @resolve_retry_count.setter
    def resolve_retry_count(self, value: int) -> None:
        self._set_var('resolve_retry_count', value)

    @property
    def please_delete(self) -> bool:
        """Whether the job should be deleted.
        """
        return bool(self._get_var('please_delete'))

    @please_delete.setter
    def please_delete(self, value: bool) -> None:
        self._set_var('please_delete', value)

    @property
    def log(self) -> str:
        """Log output as of last update.
        """
        result = ''
        cursor = self._store._thread_local_data.conn.execute(
            """
            SELECT level, time, message FROM job_log
            WHERE job_id = ? or job_id IS NULL
            ORDER BY time ASC""", (self.id, ))
        for row in cursor:
            level_str = logging.getLevelName(row[0])
            time_str = asctime(localtime(row[1]))
            message = row[2]
            result += '{} {}: {}\n'.format(time_str, level_str, message)
        cursor.close()

        return result

    @property
    def remote_output(self) -> str:
        """cwl-runner output as of last update.
        """
        return cast(str, self._get_var('remote_output'))

    @remote_output.setter
    def remote_output(self, value: str) -> None:
        self._set_var('remote_output', value)

    @property
    def remote_error(self) -> str:
        """cwl-runner stderr output as of last update.
        """
        return cast(str, self._get_var('remote_error'))

    @remote_error.setter
    def remote_error(self, value: str) -> None:
        self._set_var('remote_error', value)

    # Post-resolving data
    @property
    def workflow_content(self) -> Optional[bytes]:
        """The content of the workflow description file, or None if it has not
        been resolved yet.
        """
        return cast(bytes, self._get_var('workflow_content'))

    @workflow_content.setter
    def workflow_content(self, value: bytes) -> None:
        self._set_var('workflow_content', value)

    @property
    def required_num_cores(self) -> int:
        """The number of cores to reserve for this workflow.
        """
        return cast(int, self._get_var('required_num_cores'))

    @required_num_cores.setter
    def required_num_cores(self, value: int) -> None:
        self._set_var('required_num_cores', value)

    @property
    def time_limit(self) -> int:
        """The time to reserve, in seconds. If 0, use cluster default.
        """
        return cast(int, self._get_var('time_limit'))

    @time_limit.setter
    def time_limit(self, value: int) -> None:
        self._set_var('time_limit', value)

    # Post-staging data
    @property
    def remote_workdir_path(self) -> str:
        """The absolute remote path of the working directory.
        """
        return cast(str, self._get_var('remote_workdir_path'))

    @remote_workdir_path.setter
    def remote_workdir_path(self, value: str) -> None:
        self._set_var('remote_workdir_path', value)

    @property
    def remote_workflow_path(self) -> str:
        """The absolute remote path of the CWL workflow file.
        """
        return cast(str, self._get_var('remote_workflow_path'))

    @remote_workflow_path.setter
    def remote_workflow_path(self, value: str) -> None:
        self._set_var('remote_workflow_path', value)

    @property
    def remote_input_path(self) -> str:
        """The absolute remote path of the input description file.
        """
        return cast(str, self._get_var('remote_input_path'))

    @remote_input_path.setter
    def remote_input_path(self, value: str) -> None:
        self._set_var('remote_input_path', value)

    @property
    def remote_stdout_path(self) -> str:
        """The absolute remote path of the standard output dump.
        """
        return cast(str, self._get_var('remote_stdout_path'))

    @remote_stdout_path.setter
    def remote_stdout_path(self, value: str) -> None:
        self._set_var('remote_stdout_path', value)

    @property
    def remote_stderr_path(self) -> str:
        """The absolute remote path of the standard error dump.
        """
        return cast(str, self._get_var('remote_stderr_path'))

    @remote_stderr_path.setter
    def remote_stderr_path(self, value: str) -> None:
        self._set_var('remote_stderr_path', value)

    # Post-destaging data
    @property
    def local_output(self) -> str:
        """The serialised JSON output object describing the
                destaged outputs.
        """
        return cast(str, self._get_var('local_output'))

    @local_output.setter
    def local_output(self, value: str) -> None:
        self._set_var('local_output', value)

    # Internal data
    @property
    def remote_job_id(self) -> str:
        """The id the remote scheduler gave to this job.
        """
        return cast(str, self._get_var('remote_job_id'))

    @remote_job_id.setter
    def remote_job_id(self, value: str) -> None:
        self._set_var('remote_job_id', value)

    def try_transition(self, from_state: JobState, to_state: JobState) -> bool:
        """Attempts to transition the job's state to a new one.

        If the current state equals from_state, it is set to to_state,
        and True is returned, otherwise False is returned and the
        current state remains what it was.

        Args:
            from_state: The expected current state
            to_state: The desired next state

        Returns:
            True iff the transition was successful.
        """
        res = self._store._thread_local_data.conn.execute(
            """
            UPDATE jobs SET state = ? WHERE job_id = ? AND state = ?;""",
            (to_state.name, self.id, from_state.name))
        self._store._thread_local_data.conn.commit()
        success = res.rowcount == 1
        res.close()
        return success

    def add_log(self, level: int, message: Union[str, List[str]]) -> None:
        """Add a message to the job's log.

        Args:
            level: Level of importance
            message: The log message.
        """
        if not isinstance(message, list):
            message = [message]
        cursor = self._store._thread_local_data.conn.cursor()
        for msg in message:
            cursor.execute(
                'INSERT INTO job_log (job_id, level, time, message)'
                'VALUES (?, ?, ?, ?)', (self.id, level, time(), msg))
        cursor.connection.commit()
        cursor.close()

    def debug(self, message: Union[str, List[str]]) -> None:
        """Add a message to the job's log at level DEBUG.

        Args:
            message: The log message.
        """
        self.add_log(logging.DEBUG, message)

    def info(self, message: Union[str, List[str]]) -> None:
        """Add a message to the job's log at level INFO.

        Args:
            message: The log message.
        """
        self.add_log(logging.INFO, message)

    def warning(self, message: Union[str, List[str]]) -> None:
        """Add a message to the job's log at level WARNING.

        Args:
            message: The log message.
        """
        self.add_log(logging.WARNING, message)

    def error(self, message: Union[str, List[str]]) -> None:
        """Add a message to the job's log at level ERROR.

        Args:
            message: The log message.
        """
        self.add_log(logging.ERROR, message)

    def critical(self, message: Union[str, List[str]]) -> None:
        """Add a message to the job's log at level CRITICAL.

        Args:
            message: The log message.
        """
        self.add_log(logging.CRITICAL, message)

    def _get_var(self, var: str) -> Union[str, int, bytes]:
        """Do NOT feed this user input for var. Static strings only."""
        cursor = self._store._thread_local_data.conn.execute(
            """
            SELECT %s FROM jobs WHERE job_id = ?""" % var, (self.id, ))
        value = cursor.fetchone()[0]
        cursor.close()
        return value

    def _set_var(self, var: str, value: Union[str, int, bytes]) -> None:
        """Do NOT feed this user input for var. Static strings only."""
        cursor = self._store._thread_local_data.conn.execute(
            """
            UPDATE jobs SET %s = ? WHERE job_id = ?""" % var, (value, self.id))
        self._store._thread_local_data.conn.commit()
        cursor.close()
