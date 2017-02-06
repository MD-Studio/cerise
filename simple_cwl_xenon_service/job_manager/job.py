from JobState import JobState
from WorkflowBinding import WorkflowBinding

class Job:
    """Class Job
    """
    # Attributes:
    id = None  # (string) 
    name = None  # (string) 
    workflow = None  # (string) 
    input = None  # (WorkflowBinding) 
    
    # Operations
    def get_state(self):
        """function get_state
        
        returns JobState
        """
        return None # should raise NotImplementedError()
    
    def get_output(self):
        """function get_output
        
        returns string
        """
        return None # should raise NotImplementedError()
    
    def get_log(self):
        """function get_log
        
        returns string
        """
        return None # should raise NotImplementedError()
    
    def cancel(self):
        """function cancel
        
        returns void
        """
        return None # should raise NotImplementedError()
    

