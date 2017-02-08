from .job_state import JobState

class Job:
    """Class Job
    """
    # Attributes:
    def __init__(self, id, name, workflow, input):
        self.id = id
        self.name = name
        self.workflow = workflow
        self.input = input
        self.state = JobState.WAITING
    
    # Operations
    def get_state(self):
        """function get_state
        
        returns JobState
        """
        # update state
        return self.state
    
    def get_output(self):
        """function get_output
        
        returns string
        """
        return "Here be output"
    
    def get_log(self):
        """function get_log
        
        returns string
        """
        return "Here be logging output"
    
    def cancel(self):
        """function cancel
        
        returns void
        """
        self.state = JobState.CANCELLED
        return None
    

