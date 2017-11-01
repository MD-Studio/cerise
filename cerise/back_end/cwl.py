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

def get_files_from_binding(cwl_binding):
    """Parses a CWL input or output binding an returns a list
    containing name: path pairs. Any non-File objects are
    omitted.

    Args:
        cwl_binding (Dict): A dict structure parsed from a JSON CWL binding

    Returns:
        List[Tuple[str, str]]: A list of (name, location) tuples,
        where name contains the input or output name, and
        location the URL.
    """
    result = []
    if cwl_binding is not None:
        for name, value in cwl_binding.items():
            if (    isinstance(value, dict) and
                    'class' in value and value['class'] == 'File'):
                result.append((name, value['location']))

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
