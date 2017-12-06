import yaml
from cerise.job_store.job_state import JobState

def is_workflow(workflow_content):
    """Takes CWL file contents and checks whether it is a CWL Workflow
    (and not an ExpressionTool or CommandLineTool).

    Args:
        workflow_content (bytes): a dict structure parsed from a CWL
                file.

    Returns:
        bool: True iff the top-level Process in this CWL file is an
                instance of Workflow.
    """
    workflow = yaml.safe_load(workflow_content)
    process_class = workflow.get('class')
    return process_class == 'Workflow'


class SecondaryFile:
    """Holds a secondary file definition."""
    def __init__(self, location):
        """Creates a SecondaryFile.

        Args:
            location (str): The URL of this file.
        """
        self.location = location
        """(str) The URL of this file."""
        self.secondary_files = []
        """([SecondaryFile]) Secondary files of this secondary file."""


def get_secondary_files(secondary_files):
    """Parses a list of secondary files.

    Args:
        secondary_files (list): A list of values from a CWL \
                secondaryFiles attribute.

    Returns:
        ([SecondaryFile]): A list of secondary files.
    """
    result = []
    for value in secondary_files:
        if isinstance(value, dict):
            if 'class' in value and value['class'] == 'File':
                new_file = SecondaryFile(value['location'])
                if 'secondaryFiles' in value:
                    new_file.secondary_files = get_secondary_files(value['secondaryFiles'])
                result.append(new_file)
            elif 'class' in value and value['class'] == 'Directory':
                raise RuntimeError("Directory inputs are not yet supported, sorry")
            else:
                raise RuntimeError("Invalid secondaryFiles entry: must be a File or a Directory")
    return result


def get_files_from_binding(cwl_binding):
    """Parses a CWL input or output binding an returns a list
    containing name: path pairs. Any non-File objects are
    omitted.

    Args:
        cwl_binding (Dict): A dict structure parsed from a JSON CWL binding

    Returns:
        (List[Tuple[str, str, SecondaryFiles]]): A list of (name, \
                location, secondary_files) tuples, where name contains \
                the input or output name, and location the URL.
    """
    result = []
    if cwl_binding is not None:
        for name, value in cwl_binding.items():
            if (    isinstance(value, dict) and
                    'class' in value and value['class'] == 'File'):
                secondary_files = get_secondary_files(value.get('secondaryFiles', []))
                result.append((name, value['location'], secondary_files))

    return result

def get_cwltool_result(cwltool_log):
    """Parses cwltool log output and returns a JobState object
    describing the outcome of the cwl execution.

    Args:
        cwltool_log (str): The standard error output of cwltool

    Returns:
        JobState: Any of JobState.PERMANENT_FAILURE,
        JobState.TEMPORARY_FAILURE or JobState.SUCCESS, or
        JobState.SYSTEM_ERROR if the output could not be interpreted.
    """
    if 'Tool definition failed validation:' in cwltool_log:
        return JobState.PERMANENT_FAILURE
    if 'Final process status is permanentFail' in cwltool_log:
        return JobState.PERMANENT_FAILURE
    elif 'Final process status is temporaryFail' in cwltool_log:
        return JobState.TEMPORARY_FAILURE
    elif 'Final process status is success' in cwltool_log:
        return JobState.SUCCESS

    return JobState.SYSTEM_ERROR
