from .job import Job
from .job_description import JobDescription
from .job_store import JobStore

from uuid import uuid4

class InMemoryJobStore:
    """A JobStore that stores jobs in memory
    """
   
    def __init__(self):
        self._jobs = []

    # Operations
    def create_job(self, description):
        """function create_job
        
        description: JobDescription
        
        returns string
        """
        job_id = uuid4().hex

        job = Job(
                id=job_id,
                name=description.name,
                workflow=description.workflow,
                input=description.input)
       
        self._jobs.append(job)

        # TODO: start it using Xenon

        return job_id
    
    def list_jobs(self):
        """function list_jobs
        
        returns Job[0..*]
        """
        return self._jobs
    
    def get_job(self, job_id):
        """function get_job
        
        job_id: string
        
        returns Job
        """
        matching_jobs = [job for job in self._jobs if job.id == job_id]
        assert len(matching_jobs) <= 1
        if not matching_jobs:
            return None
        return matching_jobs[0]
    
    def delete_job(self, job_id):
        """function delete_job
        
        job_id: string
        
        returns void
        """
        self._jobs = [job for job in self._jobs if job.id != job_id]
