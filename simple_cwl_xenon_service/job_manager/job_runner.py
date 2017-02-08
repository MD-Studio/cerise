from .job_store import JobStore

class JobRunner:
    """Abstract base class for job runners.
    """
    def __init__(self, job_store):
        self.job_store = job_store

    def update(self):
        """function update
        description: Update the status of all jobs in the store to their current
        values.
        """
        raise NotImplementedError()

    def start_job(self, job_id):
        """function start_job
        description: Start executing the given job
        """

