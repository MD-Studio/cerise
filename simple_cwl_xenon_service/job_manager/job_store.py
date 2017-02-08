from .job import Job
from .job_description import JobDescription

class JobStore:
    """Abstract class JobStore
    """
    # Attributes:
    
    # Operations
    def create_job(self, description):
        """function create_job
        
        description: JobDescription
        
        returns string
        """
        raise NotImplementedError()
    
    def list_jobs(self):
        """function list_jobs
        
        returns Job[0..*]
        """
        raise NotImplementedError()
    
    def get_job(self, job_id):
        """function get_job
        
        job_id: string
        
        returns Job
        """
        raise NotImplementedError()
    
    def delete_job(self, job_id):
        """function delete_job
        
        job_id: string
        
        returns void
        """
        raise NotImplementedError()
    

