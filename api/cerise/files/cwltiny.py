#!/usr/bin/env python3

import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile

from urllib.parse import urlparse


# Logging and output

def log(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def exit_validation(message):
    log(message)
    log('Tool definition failed validation')
    sys.exit(1)

def exit_perm_fail(message):
    log(message)
    log('Final process status is permanentFail')
    sys.exit(1)

def exit_system_error(message):
    log(message)
    log('Encountered a system error')
    sys.exit(1)

def exit_success():
    log('Final process status is success')
    sys.exit(0)

# CWL process type detection
def process_type(process_dict):
    """Return the type of process described by the given CWL process
    dict.

    Args:
        process_dict (dict): A CWL process dict

    Returns:
        str: 'Workflow', 'CommandLineTool', or whatever else is in the
                class field.
    """
    if 'class' not in process_dict:
        exit_perm_fail("No class attribute in process")
    return process_dict['class']


# CWL normalisation

def normalise_parameter(dict_of_dicts):
    """Convert a dict of dicts representing CWL Parameters to a list
    of dicts, with the keys of the original outer dict inserted into
    the corresponding inner dict as the value of the 'id' key.

    If the input is a string, converts to a list of dicts with the
    key in the 'id' key and the value in the 'type' key.

    Note that input and output share inner dicts where they exist, so
    the argument also gets 'id' members.

    Args:
        dict_of_dicts (dict): A dictionary of dictionaries

    Returns:
        (list): A list of dictionaries
    """
    new_inputs = []
    for key, inner_dict in dict_of_dicts.items():
        if not isinstance(inner_dict, dict):
            inner_dict = { 'type': inner_dict }
        inner_dict['id'] = key
        new_inputs.append(inner_dict)
    return new_inputs

def normalise_process(process_desc):
    """CWL allows syntactic sugar in inputs and outputs; convert any
    dict that uses this to a canonical one that doesn't, so that we
    have predictible input for the rest of the processing.

    Args:
        clt_desc (dict): The CWL Process dict/list structure

    Returns:
        dict: A CWL Process dict/list structure
    """
    if not 'inputs' in process_desc:
        exit_validation("Error: no inputs defined for Process")

    if isinstance(process_desc['inputs'], dict):
        process_desc['inputs'] = normalise_parameter(process_desc['inputs'])

    if not 'outputs' in process_desc:
        exit_validation("Error: no outputs defined for Process")

    if isinstance(process_desc['outputs'], dict):
        process_desc['outputs'] = normalise_parameter(process_desc['outputs'])


# Command line tool execution

_workdirs = []
def make_workdir():
    """Create a temporary working directory and register it so that
    it can be deleted on exit.

    Returns:
        str: The path of the newly created temporary directory.
    """
    workdir = tempfile.mkdtemp(prefix='cerise_runner_')
    _workdirs.append(workdir)
    return workdir

def remove_workdirs():
    """Remove all workdirs that were made during workflow execution.
    """
    for workdir in _workdirs:
        shutil.rmtree(workdir, ignore_errors=True)

def stage_input_file(workdir_path, files):
    """Stage an input file into the working directory whose path
    is in workdir_path. Uses the basename if given. Recursively
    stages secondary files.

    Adds a 'path' key with the path to the File objects in files.

    Args:
        workdir_path (str): Path to the working directory
        files (Union[dict,[dict]]): A dictionary with a CWL File \
                object, or a list of such.
    """
    if not isinstance(files, list):
        files = [files]

    for file_dict in files:
        location = urlparse(file_dict['location'])
        if 'basename' in file_dict:
            dest_path = os.path.join(workdir_path, file_dict['basename'])
        else:
            dest_path = os.path.join(workdir_path, os.path.basename(location.path))
        shutil.copy(location.path, dest_path)
        file_dict['path'] = dest_path

        for i, secondary_file in enumerate(file_dict.get('secondaryFiles', [])):
            stage_input_file(workdir_path, file_dict['secondaryFiles'][i])

def stage_input(workdir_path, input_dict):
    """Stage input files described in input_dict into the working
    directory whose path is in workdir_path. Uses the basename if
    given.

    Args:
        workdir_path (str): Path to the working directory
        input_dict (dict): A dictionary with input names as keys and
                string or dicts with CWL File dicts as values.
    """
    for input_name, input_value in input_dict.items():
        if not isinstance(input_value, list):
            input_value = [input_value]
        for obj in input_value:
            if isinstance(obj, dict):
                if 'class' not in obj:
                    exit_perm_fail('Error: missing class in input ' + input_name)
                if obj['class'] == 'Directory':
                    exit_system_error('Sorry: I don''t know how to deal with directories yet')
                if obj['class'] == 'File':
                    stage_input_file(workdir_path, obj)

def create_argument(parameter, input_dict):
    """Create a command line argument from the given parameter of the
    process, taking input from the given input object.

    Args:
        parameter (dict): A dict with a CommandInputParameter structure
        input_dict (dict): An input dict object

    Returns:
        (int, [str]): The position of the argument and the items
                comprising it
    """
    arg = []
    position = 0

    if 'id' not in parameter:
        exit_perm_fail("Error: input parameter given without an id")
    par_id = parameter['id']

    # get parameter type properties
    par_type = parameter.get('type')

    is_optional = False
    if par_type.endswith('?'):
        is_optional = True
        par_type = par_type[0:-1]

    is_array = False
    if par_type.endswith('[]'):
        is_array = True
        par_type = par_type[0:-2]

    # get input value
    value = parameter.get('default')
    if par_id in input_dict:
        value = input_dict[par_id]

    # check type a bit
    if not is_optional and value is None:
        exit_perm_fail("Error: no input provided for required parameter {}".format(str(par_id)))
    if is_array and not isinstance(value, list):
        exit_perm_fail("Error: expected an array input value for parameter {}".format(str(par_id)))

    if 'inputBinding' in parameter and value is not None:
        binding = parameter['inputBinding']

        # get argument creation settings
        separate = 'separate' not in binding or binding['separate']
        item_separator = binding.get('itemSeparator')
        prefix = binding.get('prefix')

        # produce argument
        if is_array:
            if par_type == 'File':
                value = list(map(lambda x: x['path'], value))
            else:
                value = list(map(str, value))

            if item_separator:
                value = [item_separator.join(value)]
        else:
            if par_type == 'File':
                value = [value['path']]
            else:
                value = [value]

        for val in value:
            if prefix:
                if separate:
                    arg.append(prefix)
                    arg.append(str(val))
                else:
                    arg.append(prefix + str(val))
            else:
                arg.append(str(val))

        # put it in the right place
        if 'position' in binding:
            position = int(binding['position'])

    return position, arg

def create_command_line(clt_desc, input_dict):
    """Create a list of command line items. Each item is a tuple of
    a number (sort key) and a list of strings (command line items).

    Args:
        clt_desc (dict): The command line tool descriptions structure
        input_dict (dict): A dictionary with input names as keys and
                strings or dicts with CWL File dicts as values
    """
    args = []
    if 'arguments' in clt_desc:
        for argument in clt_desc['arguments']:
            if not isinstance(argument, str):
                exit_system_error('Sorry: I only understand strings for arguments.'
                        'Please use the inputs to pass arguments from input parameters.')
        args.append((-1, clt_desc['arguments']))

    for parameter in clt_desc['inputs']:
        args.append(create_argument(parameter, input_dict))

    args.sort(key=lambda arg: arg[0])

    # drop keys and flatten
    command_line = []
    for _, items in args:
        if items is not None:
            command_line.extend(items)
    return command_line

def execute_clt(workdir_path, in_out, base_command, command_line):
    """Execute a command line tool.

    Args:
        workdir_path (str): Path to the working directory to run in
        in_out (dict): A dictionary mapping stream names to file names
                or to None for standard in and out and standard error
        base_command (str): The base command to run, or None to use
                first command_line item
        command_line ([str]): Command line options to add
    """
    if base_command is not None:
        command_line.insert(0, base_command)

    stdin_file = subprocess.DEVNULL
    if in_out['stdin'] is not None:
        stdin_path = os.path.join(workdir_path, in_out['stdin'])
        stdin_file = open(stdin_path, 'rb')
    stdout_file = subprocess.DEVNULL
    if in_out['stdout'] is not None:
        stdout_path = os.path.join(workdir_path, in_out['stdout'])
        log("Writing stdout to " + stdout_path)
        stdout_file = open(stdout_path, 'wb')
    stderr_file = subprocess.DEVNULL
    if in_out['stderr'] is not None:
        stderr_path = os.path.join(workdir_path, in_out['stderr'])
        stderr_file = open(stderr_path, 'wb')

    log("Command line: " + str(command_line))
    result = subprocess.call(command_line,
            cwd=workdir_path,
            stdin=stdin_file,
            stdout=stdout_file,
            stderr=stderr_file)

    log("Ran subprocess")
    log("Result: " + str(result))
    return result

def collect_output(workdir_path, outputs):
    """Collect output files for return to caller.

    Args:
        workdir_path(str): Path to the working directory to run in
        outputs ([dict]): List of OutputParameter dict objects

    Returns:
        dict: Dictionary mapping output ids to File objects
    """
    output_dict = {}
    for output_parameter in outputs:
        if 'id' not in output_parameter:
            exit_validation("Error: output without id member")
        if 'type' not in output_parameter:
            exit_validation("Error: output without type member")
        if output_parameter['type'] != 'File':
            exit_system_error("Sorry, I only know about File outputs")
        if 'outputBinding' in output_parameter:
            binding = output_parameter['outputBinding']
            paths = []
            if 'glob' in binding:
                paths = glob.glob(os.path.join(workdir_path, binding['glob']))
            log("Paths after globbing: " + str(paths))
            if paths != []:
                output_dict[output_parameter['id']] = {
                        'class': 'File',
                        'location': 'file:///' + paths[0]
                        }
    return output_dict

def destage_output(output_dict):
    """Gets output files from the temporary working directory and
    moves (!) them to the current directory, so that we don't lose
    the files when the tempdir is deleted later.

    Args:
        output_dict (dict): A dict structure describing the output.

    Returns:
        dict: A dict structure describing the output in the new
                location.
    """
    for _, desc in output_dict.items():
        if isinstance(desc, dict):
            if desc['class'] == 'File':
                location = urlparse(desc['location'])
                dest_path = os.path.join(os.getcwd(), os.path.basename(location.path))
                shutil.move(location.path, dest_path)
                desc['location'] = 'file://' + dest_path

    return output_dict

def run_command_line_tool(workdir_path, clt_dict, input_dict):
    """Executes a command line tool as described by a CommandLineTool
    object described in a CWL file (in JSON form).

    Args:
        clt_path (str): The path to the CWL file describing the tool
                to run.
        input_dict (dict): A dictionary describing inputs
    """
    log("Running command line tool {}\n with input {}".format(
            json.dumps(clt_dict, indent=4),
            json.dumps(input_dict, indent=4)))
    has_error = False
    normalise_process(clt_dict)
    stage_input(workdir_path, input_dict)
    command_line = create_command_line(clt_dict, input_dict)
    base_command = clt_dict.get('baseCommand')
    in_out = {
            'stdin': clt_dict.get('stdin'),
            'stdout': clt_dict.get('stdout'),
            'stderr': clt_dict.get('stderr')
            }
    result = execute_clt(workdir_path, in_out, base_command, command_line)
    if result != 0:
        has_error = True
    output_dict = collect_output(workdir_path, clt_dict['outputs'])
    return has_error, output_dict


# Workflow execution

def normalise_workflow(workflow_dict):
    """Normalise away syntactic sugar for a workflow's inputs, outputs,
    and steps.

    Args:
        workflow_dict (dict): A dict representing a CWL Workflow
                document.
    """
    normalise_process(workflow_dict)
    if not 'steps' in workflow_dict:
        exit_perm_fail("No steps in Workflow")

    if isinstance(workflow_dict['steps'], dict):
        new_steps = []
        for step_id, step in workflow_dict['steps'].items():
            step['id'] = step_id
            new_steps.append(step)
        workflow_dict['steps'] = new_steps

    for step in workflow_dict['steps']:
        if 'in' in step:
            if isinstance(step['in'], dict):
                new_in = []
                for key, value in step['in'].items():
                    if isinstance(value, str):
                        new_in.append({'id': key, 'source': value})
                    elif isinstance(value, dict):
                        value['id'] = key
                        new_in.append(value)
                step['in'] = new_in

        if 'out' in step:
            if not isinstance(step['out'], list):
                exit_perm_fail("The out attribute of a workflow step must be an array")
            for i, output in enumerate(step['out']):
                if isinstance(output, str):
                    step['out'][i] = {'id': output}

def has_unexecuted_steps(workflow_dict):
    """Returns whether the workflow has unexecuted steps. Uses the
    cwltiny_output_available tag on the step to test.

    Args:
        workflow_dict (dict): A dict representing a CWL workflow
                document being executed.

    Returns:
        bool: True iff there are unexecuted steps in the workflow.
    """
    for step in workflow_dict['steps']:
        if not 'cwltiny_output_available' in step:
            log("\nFound unexecuted step " + step['id'])
            return True
    return False

def resolve_output_reference(reference, workflow_dict, input_dict):
    """Get the output value (if any) corresponding to the reference
    given. References may be a string with no /, referring to a value
    in the input, or one with a /, referring to the output of a step.

    Args:
        reference (str): An output reference
        workflow_dict (dict): A CWL Workflow document
        input_dict (dict): An input document

    Returns:
        (Union[dict, None], bool): Either a tuple (value, True) where \
                value may be None if the output is optional and \
                missing, or a tuple (None, False) if the output is not \
                yet available.
    """
    log("Resolving reference " + str(reference))
    source = reference.split(sep='/')
    if len(source) == 1:
        if reference not in input_dict:
            input_def = [d for d in workflow_dict['inputs'] if d['id'] == reference]
            if input_def:
                input_def = input_def[0]
                if 'default' in input_def:
                    return input_def['default'], True
                else:
                    if 'type' in input_def and input_def['type'].endswith('?'):
                        return None, True
                    else:
                        exit_perm_fail("No input and no default for required input {}".format(reference))
            else:
                exit_perm_fail("Source reference {} not found".format(reference))
            exit_perm_fail("Could not resolve input " + reference)
        return input_dict[reference], True

    if len(source) != 2:
        exit_perm_fail("Source reference with more than one /")

    step_id = source[0]
    output_id = source[1]
    for step in workflow_dict['steps']:
        if 'id' in step and step['id'] == step_id:
            if 'cwltiny_output_available' not in step:
                return None, False
            for output in step['out']:
                if output['id'] == output_id:
                    return output['cwltiny_value'], True

def resolve_step_inputs(step, workflow_dict, input_dict):
    """Get input values for steps where available, and put them into
    a dict under cwltiny_input_values on the step.

    Args:
        step (dict): A WorkflowStep in workflow_dict
        workflow_dict (dict): A dict representing a CWL workflow
                document being executed.
        input_dict (dict): The input data to use

    Returns:
        bool: True iff all inputs for this step are bound
    """
    log("Resolving step '{}'".format(step['id']))
    all_bound = True
    if 'cwltiny_input_values' not in step:
        step['cwltiny_input_values'] = {}

    if 'in' in step:
        for step_input in step['in']:
            value = None
            if 'source' in step_input:
                value, ready = resolve_output_reference(step_input['source'], workflow_dict, input_dict)

            log("Resolved step '{}' input '{}' to {}".format(step['id'], step_input['id'], json.dumps(value, indent=4)))

            step['cwltiny_input_values'][step_input['id']] = value
            if not ready:
                all_bound = False

    return all_bound

def execute_workflow_step(step):
    """Execute a CWL workflow step.

    Args:
        step (dict): A WorkflowStep to execute
    """
    if 'run' in step:
        log("\nRunning workflow step '{}' from file '{}'".format(step['id'], step['run']))
        run_dict = json.load(open(step['run'], 'r'))
        workdir_path = make_workdir()
        input_dict = step['cwltiny_input_values']
        if process_type(run_dict) == 'Workflow':
            has_error, output_dict = run_workflow(workdir_path, run_dict, input_dict)
        elif process_type(run_dict) == 'CommandLineTool':
            has_error, output_dict = run_command_line_tool(workdir_path, run_dict, input_dict)

        log("Step output: {}\n".format(json.dumps(output_dict, indent=4)))

        if 'out' in step:
            for output in step['out']:
                output['cwltiny_value'] = output_dict.get(output['id'])

    step['cwltiny_output_available'] = True
    return has_error


def get_workflow_outputs(workflow_dict, input_dict):
    """Get the outputs as described by the workflow from the values
    stored on the steps.

    Missing outputs will be silently ignored, they're just not added
    to the output dict at all.

    Args:
        workflow_dict (dict): The workflow to get outputs of
        input_dict (dict): The input data for the workflow

    Returns:
        dict: A dict with output ids for keys, and the corresponding
                values as values
    """
    output_dict = {}
    for output_parameter in workflow_dict['outputs']:
        if 'outputSource' in output_parameter:
            value, found = resolve_output_reference(
                    output_parameter['outputSource'], workflow_dict, input_dict)
            if found:
                output_dict[output_parameter['id']] = value
    return output_dict

def run_workflow(workdir_path, workflow_dict, input_dict):
    """Run a CWL workflow.

    Args:
        workdir_path (str): Path to a temp dir to work in
        workflow_dict (dict): The workflow to execute
        input_dict (dict): The input data to use
    """
    normalise_workflow(workflow_dict)
    log("Normalised workflow: " + json.dumps(workflow_dict, indent=4))
    log("Input: " + json.dumps(input_dict, indent=4))
    has_error = False
    while has_unexecuted_steps(workflow_dict) and not has_error:
        for step in workflow_dict['steps']:
            if not step.get('cwltiny_output_available', False):
                log("Trying to resolve unexecuted step '{}'".format(step['id']))
                all_bound = resolve_step_inputs(step, workflow_dict, input_dict)
                if all_bound:
                    step_error = execute_workflow_step(step)
                    if step_error:
                        has_error = True
                    break

    return has_error, get_workflow_outputs(workflow_dict, input_dict)


def main():
    parser = argparse.ArgumentParser(description='Process a CWL workflow')
    parser.add_argument('cwlfile', type=str, help='A CWL file in JSON format')
    parser.add_argument('inputfile', type=str, help='An input file in JSON format')

    args = parser.parse_args()

    log('====================')
    log('CWLTiny starting run')
    log('====================')

    log('CWL file: ' + args.cwlfile)
    input_dict = json.load(open(args.inputfile, 'r'))
    cwl_dict = json.load(open(args.cwlfile, 'r'))
    workdir_path = make_workdir()

    proc_type = process_type(cwl_dict)

    if proc_type == 'CommandLineTool':
        has_error, output_dict = run_command_line_tool(workdir_path, cwl_dict, input_dict)
    elif proc_type == 'Workflow':
        has_error, output_dict = run_workflow(workdir_path, cwl_dict, input_dict)

    output_dict = destage_output(output_dict)
    print(json.dumps(output_dict))

    if has_error:
        exit_perm_fail("An error occured during execution")

    remove_workdirs()

    log('====================')
    log('CWLTiny ending run')
    log('====================')
    exit_success()

if __name__ == '__main__':
    main()
