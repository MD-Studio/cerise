import logging
import sqlite3
import threading
from time import time
from types import TracebackType
from typing import Any, List, Optional
from uuid import uuid4

from cerise.job_store.job_state import JobState
from cerise.job_store.sqlite_job import SQLiteJob
from cerise.util import BaseExceptionType


class JobNotFound(RuntimeError):
    pass


class SQLiteJobStore:
    """A JobStore that stores jobs in a SQLite database.
    You must acquire the store to do anything with it or
    the jobs stored in it. It's a context manager, so
    use a with statement:

    with self._store:
        job = self._store.get_job(id)
        # go ahead and modify job
    # don't touch self._store or keep any references to jobs

    Having multiple nested with statements is okay, so you
    can call other functions that use the store and acquire
    it themselves without incident.

    Args:
        dbfile (str): The path to the file storing the database.
    """

    def __init__(self, dbfile: str) -> None:
        self._db_file = dbfile
        """The location of the database file."""

        self._pool_lock = threading.RLock()
        """A lock protecting the connection pool."""

        self._connection_pool = []  # type: List[Any]
        """The list of available database connections."""

        self._thread_local_data = threading.local()
        """Thread-local data, for storing current connection in
        acquired stores. That will be in self._thread_local_data.conn
        """

        conn = sqlite3.connect(self._db_file)
        conn.execute("""CREATE TABLE IF NOT EXISTS jobs(
                job_id CHARACTER(32),
                name VARCHAR(255),
                workflow VARCHAR(255),
                local_input TEXT,
                state VARCHAR(17) DEFAULT 'SUBMITTED',
                please_delete INTEGER DEFAULT 0,
                resolve_retry_count INTEGER DEFAULT 0,
                remote_output TEXT DEFAULT '',
                remote_error TEXT DEFAULT '',
                workflow_content BLOB,
                required_num_cores INTEGER DEFAULT 0,
                time_limit INTEGER DEFAULT 0,
                remote_workdir_path VARCHAR(255) DEFAULT '',
                remote_workflow_path VARCHAR(255) DEFAULT '',
                remote_input_path VARCHAR(255) DEFAULT '',
                remote_stdout_path VARCHAR(255) DEFAULT '',
                remote_stderr_path VARCHAR(255) DEFAULT '',
                remote_system_out_path VARCHAR(255) DEFAULT '',
                remote_system_err_path VARCHAR(255) DEFAULT '',
                remote_job_id VARCHAR(255),
                local_output TEXT DEFAULT ''
                )
                """)
        conn.execute("""CREATE TABLE IF NOT EXISTS job_log(
                job_id CHARACTER(32),
                level INTEGER,
                time DOUBLE PRECISION,
                message TEXT
                )
                """)
        conn.commit()
        conn.close()

    def __enter__(self) -> 'SQLiteJobStore':
        """Grabs a connection from the shared connection pool, and
        puts it in thread-local storage, thus reserving it for the
        present thread, which will use it for any subsequent actions
        on this SQLiteJobStore or the SQLiteJobs obtained from it.

        Creates a connection if none are available, the number of
        connections is automatically limited by the number of active
        threads.

        Takes into account possible recursion by refcounting.
        """
        if 'conn' not in self._thread_local_data.__dict__:
            self._pool_lock.acquire()

            if self._connection_pool != []:
                self._thread_local_data.conn = self._connection_pool.pop()
            else:
                self._thread_local_data.conn = sqlite3.connect(
                    self._db_file, isolation_level="IMMEDIATE")

            self._thread_local_data.recursion_depth = 1

            self._pool_lock.release()

        else:
            self._thread_local_data.recursion_depth += 1
        return self

    def __exit__(self, exc_type: Optional[BaseExceptionType],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> None:
        """Returns the connection back to the pool.
        """
        if self._thread_local_data.recursion_depth == 1:
            self._pool_lock.acquire()

            connection = self._thread_local_data.__dict__.pop('conn')

            # Roll back any open transaction so we don't keep the DB
            # locked forever if an error occurs.
            connection.rollback()

            self._connection_pool.append(connection)

            self._pool_lock.release()

        self._thread_local_data.recursion_depth -= 1

    def create_job(self, name: str, workflow: str, job_input: str) -> str:
        """Create a job.

        Args:
            name: The user-assigned name of the job
            workflow: A string containing a URL pointing to the
                workflow
            job_input: A string containing a json description of
                a json string.

        Returns:
            A string containing the job id.
        """
        job_id = uuid4().hex

        cursor = self._thread_local_data.conn.execute(
            """
                INSERT INTO jobs (job_id, name, workflow, local_input, state)
                VALUES (?, ?, ?, ?, ?)""",
            (job_id, name, workflow, job_input, JobState.SUBMITTED.name))
        cursor.execute(
            'INSERT INTO job_log (job_id, level, time, message)'
            'VALUES (?, ?, ?, ?)', (job_id, logging.INFO, time(),
                                    'Submitted job'))
        self._thread_local_data.conn.commit()
        cursor.close()

        return job_id

    def list_jobs(self) -> List[SQLiteJob]:
        """Return a list of all currently known jobs.

        Returns:
            A list of SQLiteJob objects.
        """
        cursor = self._thread_local_data.conn.execute("""
                SELECT job_id FROM jobs;""")
        ret = [SQLiteJob(self, row[0]) for row in cursor.fetchall()]
        cursor.close()
        return ret

    def get_job(self, job_id: str) -> SQLiteJob:
        """Return the job with the given id.

        Args:
            job_id: A string containing a job id, as obtained from
                create_job() or list_jobs().

        Returns:
            The job object corresponding to the given id.
        """
        cursor = self._thread_local_data.conn.execute(
            """
                SELECT COUNT(*) FROM jobs WHERE job_id = ?""", (job_id, ))
        not_found = cursor.fetchone()[0] == 0
        cursor.close()

        if not_found:
            raise JobNotFound(
                'Job with id {} not found in store'.format(job_id))
        return SQLiteJob(self, job_id)

    def delete_job(self, job_id: str) -> None:
        """Delete the job with the given id.

        Args:
            job_id: A string containing the id of the job to be deleted.
        """
        cursor = self._thread_local_data.conn.execute(
            """
                DELETE FROM jobs WHERE job_id = ?""", (job_id, ))
        cursor.execute(
            'DELETE FROM job_log WHERE job_id = ?', (job_id, ))
        self._thread_local_data.conn.commit()
        cursor.close()
