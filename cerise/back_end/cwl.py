from typing import Any, Dict, List

import yaml

from cerise.back_end.file import File
from cerise.job_store.job_state import JobState


def is_workflow(workflow_content: bytes) -> bool:
    """Takes CWL file contents and checks whether it is a CWL Workflow
    (and not an ExpressionTool or CommandLineTool).

    Args:
        workflow_content: a dict structure parsed from a CWL file.

    Returns:
        True iff the top-level Process in this CWL file is an
                instance of Workflow.
    """
    try:
        workflow = yaml.safe_load(workflow_content.decode())
    except yaml.scanner.ScannerError:  # type: ignore
        return False
    except yaml.parser.ParserError:  # type: ignore
        return False

    if 'class' not in workflow:
        return False
    if 'inputs' not in workflow or 'outputs' not in workflow:
        return False
    if 'steps' not in workflow:
        return False

    process_class = workflow.get('class')
    return process_class == 'Workflow'


def get_workflow_step_names(workflow_content: bytes) -> List[str]:
    """Takes a CWL workflow and extracts names of steps.

    This assumes that the steps are not inlined, but referenced by
    name, as we require for workflows submitted to Cerise. Also, this
    is not the name of the step in the workflow document, but the name
    of the step in the API to run. It's the content of the ``run``
    attribute, not that of the ``id`` attribute.

    Args:
        workflow_content: The contents of the workflow file.

    Returns:
        A list of step names.
    """
    workflow = yaml.safe_load(workflow_content.decode())
    if 'class' not in workflow or workflow['class'] != 'Workflow':
        raise RuntimeError('Invalid workflow file')
    if 'steps' not in workflow:
        raise RuntimeError('Invalid workflow file')

    steps = None
    if isinstance(workflow['steps'], dict):
        steps = list(workflow['steps'].values())
    elif isinstance(workflow['steps'], list):
        steps = workflow['steps']

    if steps is None:
        raise RuntimeError('Invalid workflow file')

    return [step['run'] for step in steps]


def get_required_num_cores(cwl_content: bytes) -> int:
    """Takes a CWL file contents and extracts number of cores required.

    Args:
        cwl_content: The contents of a CWL file.

    Returns:
        The number of cores required, or 0 if not specified.
    """
    workflow = yaml.safe_load(cwl_content.decode())
    hints = workflow.get('hints')
    if hints is None:
        return 0

    resource_requirement = hints.get('ResourceRequirement')
    if resource_requirement is None:
        return 0

    cores_min = resource_requirement.get('coresMin')
    cores_max = resource_requirement.get('coresMax')

    if cores_min is not None:
        return cores_min
    if cores_max is not None:
        return cores_max
    return 0


def get_time_limit(cwl_content: bytes) -> int:
    """Takes a CWL file contents and extracts cwl1.1-dev1 time limit.

    Supports only two of three possible ways of writing this. Returns
    0 if no value was specified, in which case the default should be
    used.

    Args:
        cwl_content: The contents of a CWL file.

    Returns:
        Time to reserve in seconds.
    """
    workflow = yaml.safe_load(cwl_content.decode())
    hints = workflow.get('hints')
    if hints is None:
        return 0

    time_limit = hints.get('TimeLimit')
    if time_limit is None:
        return 0

    if isinstance(time_limit, int):
        return time_limit
    elif isinstance(time_limit, dict):
        limit = time_limit.get('timeLimit')
        if limit is None:
            raise ValueError('Invalid TimeLimit specification in CWL file,'
                             ' expected timeLimit attribute')
        return limit
    else:
        raise ValueError('Invalid TimeLimit specification in CWL file,'
                         ' expected int or timeLimit attribute')


def get_secondary_files(secondary_files: List[Dict[str, Any]]) -> List[File]:
    """Parses a list of secondary files, recursively.

    Args:
        secondary_files: A list of values from a CWL secondaryFiles
                attribute.

    Returns:
        A list of secondary input files.
    """
    result = []
    for value in secondary_files:
        if isinstance(value, dict):
            if 'class' in value and value['class'] == 'File':
                sf = []  # type: List[File]
                if 'secondaryFiles' in value:
                    sf = get_secondary_files(value['secondaryFiles'])
                new_file = File(None, None, value['location'], sf)
                result.append(new_file)
            elif 'class' in value and value['class'] == 'Directory':
                raise RuntimeError(
                    'Directory inputs are not yet supported, sorry')
            else:
                raise RuntimeError(
                    'Invalid secondaryFiles entry: must be a File or a'
                    ' Directory'
                )
    return result


def get_files_from_binding(cwl_binding: Dict[str, Any]) -> List[File]:
    """Parses a CWL input or output binding an returns a list
    containing name: path pairs. Any non-File objects are
    omitted.

    Args:
        cwl_binding: A dict structure parsed from a JSON CWL binding

    Returns:
        A list of File objects describing the input files described
            in the binding.
    """
    result = []
    if cwl_binding is not None:
        for name, value in cwl_binding.items():
            if isinstance(value, dict):
                if value.get('class') == 'File':
                    secondary_files = get_secondary_files(
                        value.get('secondaryFiles', []))
                    result.append(
                        File(name, None, value['location'], secondary_files))
                elif value.get('class') == 'Directory':
                    raise RuntimeError(
                        'Directory inputs are not yet supported, sorry')
            elif isinstance(value, list):
                for i, val in enumerate(value):
                    if isinstance(val, dict):
                        if val.get('class') == 'File':
                            secondary_files = get_secondary_files(
                                val.get('secondaryFiles', []))
                            input_file = File(name, i, val['location'],
                                              secondary_files)
                            result.append(input_file)
                        elif val.get('class') == 'Directory':
                            raise RuntimeError(
                                'Directory inputs are not yet supported, sorry'
                            )

    return result


def get_cwltool_result(cwltool_log: str) -> JobState:
    """Parses cwltool log output and returns a JobState object
    describing the outcome of the cwl execution.

    Args:
        cwltool_log: The standard error output of cwltool

    Returns:
        Any of JobState.PERMANENT_FAILURE, JobState.TEMPORARY_FAILURE or
        JobState.SUCCESS, or JobState.SYSTEM_ERROR if the output could
        not be interpreted.
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
