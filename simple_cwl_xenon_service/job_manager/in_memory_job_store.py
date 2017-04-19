from .job import Job
from .job_description import JobDescription
from .job_store import JobStore

import threading
from uuid import uuid4

class InMemoryJobStore(JobStore):
    """A JobStore that stores jobs in memory.
    If you're running in a multithreaded environment, you should
    acquire the store. It's a context manager, so use a with
    statement:

    with self._store:
        job = self._store.get_job(id)
        # go ahead and modify job
    # don't touch self._store or keep any references to jobs

    Also it's a recursive lock, so having multiple nested with
    statements is okay.
    """

    def __init__(self):
        self._jobs = []
        """A list of Job objects."""

        self._lock = threading.RLock()
        """A lock protecting the store."""

    def __enter__(self):
        self._lock.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self._lock.release()


    def create_job(self, description):
        """Create a job.

        Args:
            description (JobDescription): A JobDescription describing the job.

        Returns:
            str: A string containing the job id.
        """
        job_id = uuid4().hex

        job = Job(
                id=job_id,
                name=description.name,
                workflow=description.workflow,
                input=description.input)

        self._jobs.append(job)

        return job_id

    def list_jobs(self):
        """Return a list of all currently known Jobs.

        Returns:
            List[Job]: A list of Job objects.
        """
        return self._jobs

    def get_job(self, job_id):
        """Return the job with the given id.

        Args:
            job_id (str): A string containing a job id, as obtained from create_job()
                or list_jobs().

        Returns:
            Job: The Job object corresponding to the given id.
        """
        matching_jobs = [job for job in self._jobs if job.id == job_id]
        assert len(matching_jobs) <= 1
        if not matching_jobs:
            return None
        return matching_jobs[0]

    def delete_job(self, job_id):
        """Delete the job with the given id.

        Args:
            job_id (str): A string containing the id of the job to be deleted.
        """
        self._jobs = [job for job in self._jobs if job.id != job_id]
