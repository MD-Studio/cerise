from .job import Job
from .job_description import JobDescription

class JobStore:
    """Abstract class JobStore. A JobStore stores a list of jobs.
    """

    # Operations
    def create_job(self, description):
        """Create a job.

        Args:
            description (JobDescription): A JobDescription describing the job.

        Returns:
            str: A string containing the job id.
        """
        raise NotImplementedError()

    def list_jobs(self):
        """Return a list of all currently known Jobs.

        Returns:
            List[Job]: A list of jobs.
        """
        raise NotImplementedError()

    def get_job(self, job_id):
        """Return the job with the given id.

        Args:
            job_id (str): A string containing a job id, as obtained from create_job()
                or list_jobs()

        Returns:
            Job: The Job object corresponding to the given id.
        """
        raise NotImplementedError()

    def delete_job(self, job_id):
        """Delete the job with the given id.

        Args:
            job_id (str): A string containing the id of the job to be deleted.
        """
        raise NotImplementedError()


