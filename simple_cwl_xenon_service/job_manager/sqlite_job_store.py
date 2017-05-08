from .sqlite_job import SQLiteJob
from .job_store import JobStore
from .job_state import JobState

import sqlite3
import threading
from uuid import uuid4

class SQLiteJobStore(JobStore):
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

    def __init__(self, dbfile):
        self._db_file = dbfile
        """The location of the database file."""

        self._pool_lock = threading.RLock()
        """A lock protecting the connection pool."""

        self._connection_pool = []
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
                log TEXT DEFAULT '',
                remote_output TEXT DEFAULT '',
                workflow_content BLOB,
                remote_workdir_path VARCHAR(255) DEFAULT '',
                remote_workflow_path VARCHAR(255) DEFAULT '',
                remote_input_path VARCHAR(255) DEFAULT '',
                remote_stdout_path VARCHAR(255) DEFAULT '',
                remote_stderr_path VARCHAR(255) DEFAULT '',
                remote_job_id VARCHAR(255),
                local_output TEXT DEFAULT ''
                )
                """)
        conn.commit()
        conn.close()

    def __enter__(self):
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
                self._thread_local_data.conn = sqlite3.connect(self._db_file, isolation_level="IMMEDIATE")

            self._thread_local_data.recursion_depth = 1

            self._pool_lock.release()

        else:
            self._thread_local_data.recursion_depth += 1

    def __exit__(self, exc_type, exc_value, traceback):
        """Returns the connection back to the pool.
        """
        if self._thread_local_data.recursion_depth == 1:
            self._pool_lock.acquire()

            connection = self._thread_local_data.__dict__.pop('conn')
            self._connection_pool.append(connection)

            self._pool_lock.release()

        self._thread_local_data.recursion_depth -= 1

    def create_job(self, description):
        """Create a job.

        Args:
            description (JobDescription): A JobDescription describing the job.

        Returns:
            str: A string containing the job id.
        """
        job_id = uuid4().hex

        self._thread_local_data.conn.execute("""
                INSERT INTO jobs (job_id, name, workflow, local_input, state)
                VALUES (?, ?, ?, ?, ?)""",
                (job_id, description.name, description.workflow,
                    description.input, JobState.SUBMITTED.name))
        self._thread_local_data.conn.commit()

        return job_id

    def list_jobs(self):
        """Return a list of all currently known jobs.

        Returns:
            List[SQLiteJob]: A list of SQLiteJob objects.
        """
        res = self._thread_local_data.conn.execute("""
                SELECT job_id FROM jobs;""")
        ret = [SQLiteJob(self, row[0]) for row in res.fetchall()]
        return ret

    def get_job(self, job_id):
        """Return the job with the given id.

        Args:
            job_id (str): A string containing a job id, as obtained from create_job()
                or list_jobs().

        Returns:
            Union[SQLiteJob, NoneType]: The job object corresponding to the given id.
        """
        res = self._thread_local_data.conn.execute("""
                SELECT COUNT(*) FROM jobs WHERE job_id = ?""", (job_id,))
        if res.fetchone()[0] == 0:
            return None
        return SQLiteJob(self, job_id)

    def delete_job(self, job_id):
        """Delete the job with the given id.

        Args:
            job_id (str): A string containing the id of the job to be deleted.
        """
        self._thread_local_data.conn.execute("""
                DELETE FROM jobs WHERE job_id = ?""",
                (job_id,))
        self._thread_local_data.conn.commit()
