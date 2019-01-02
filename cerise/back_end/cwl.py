import yaml
from cerise.job_store.job_state import JobState
from .input_file import InputFile

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
    try:
        workflow = yaml.safe_load(workflow_content)
    except yaml.scanner.ScannerError:
        return False
    except yaml.parser.ParserError:
        return False

    if not 'class' in workflow:
        return False
    if not 'inputs' in workflow or not 'outputs' in workflow:
        return False
    if not 'steps' in workflow:
        return False

    process_class = workflow.get('class')
    return process_class == 'Workflow'


def get_workflow_step_names(workflow_content):
    """Takes a CWL workflow and extracts names of steps.

    This assumes that the steps are not inlined, but referenced by
    name, as we require for workflows submitted to Cerise. Also, this
    is not the name of the step in the workflow document, but the name
    of the step in the API to run. It's the content of the ``run``
    attribute, not that of the ``id`` attribute.

    Args:
        workflow_content (bytes): The contents of the workflow file.

    Returns:
        (List[str]): A list of step names.
    """
    workflow = yaml.safe_load(workflow_content)
    if not 'class' in workflow or workflow['class'] != 'Workflow':
        raise RuntimeError('Invalid workflow file')
    if not 'steps' in workflow:
        raise RuntimeError('Invalid workflow file')

    steps = None
    if isinstance(workflow['steps'], dict):
        steps = workflow['steps'].values()
    elif isinstance(workflow['steps'], list):
        steps = workflow['steps']

    if steps is None:
        raise RuntimeError('Invalid workflow file')

    return [step['run'] for step in steps]


def get_required_num_cores(cwl_content):
    """Takes a CWL file contents and extracts number of cores required.

    Args:
        cwl_content (bytes): The contents of a CWL file.

    Returns:
        int: The number of cores required, or 0 if not specified.
    """
    workflow = yaml.safe_load(cwl_content)
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


def get_time_limit(cwl_content):
    """Takes a CWL file contents and extracts cwl1.1-dev1 time limit.

    Supports only two of three possible ways of writing this. Returns
    0 if no value was specified, in which case the default should be
    used.

    Args:
        cwl_content (bytes): The contents of a CWL file.

    Returns:
        int: Time to reserve in seconds.
    """
    workflow = yaml.safe_load(cwl_content)
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


def get_secondary_files(secondary_files):
    """Parses a list of secondary files, recursively.

    Args:
        secondary_files (list): A list of values from a CWL \
                secondaryFiles attribute.

    Returns:
        ([InputFile]): A list of secondary input files.
    """
    result = []
    for value in secondary_files:
        if isinstance(value, dict):
            if 'class' in value and value['class'] == 'File':
                new_file = InputFile(None, value['location'], None, [])
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
        [InputFile]: A list of InputFile objects describing the input \
                files described in the binding.
    """
    result = []
    if cwl_binding is not None:
        for name, value in cwl_binding.items():
            if isinstance(value, dict):
                if value.get('class') == 'File':
                    secondary_files = get_secondary_files(value.get('secondaryFiles', []))
                    result.append(InputFile(name, value['location'], None, secondary_files))
                elif value.get('class') == 'Directory':
                    raise RuntimeError('Directory inputs are not yet supported, sorry')
            elif isinstance(value, list):
                for i, val in enumerate(value):
                    if isinstance(val, dict):
                        if val.get('class') == 'File':
                            secondary_files = get_secondary_files(val.get('secondaryFiles', []))
                            input_file = InputFile(name, val['location'], None, secondary_files, i)
                            result.append(input_file)
                        elif val.get('class') == 'Directory':
                            raise RuntimeError('Directory inputs are not yet supported, sorry')

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
