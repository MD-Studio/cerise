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

def exit_success(message):
    log(message)
    log('Final process status is success')
    sys.exit(1)


# CWL normalisation

def normalise_dict_list(dict_of_dicts):
    """Convert a dict of dicts to a list of dicts, with the keys
    of the original outer dict inserted into the corresponding
    inner dict as the value of the 'id' member.

    Note that input and output share inner dicts, so the argument
    also gets 'id' members.

    Args:
        dict_of_dicts (dict): A dictionary of dictionaries

    Returns:
        (list): A list of dictionaries
    """
    new_inputs = []
    for key, inner_dict in dict_of_dicts.items():
        inner_dict['id'] = key
        new_inputs.append(inner_dict)
    return new_inputs

def normalise_command_line_tool(clt_desc):
    """CWL allows syntactic sugar in inputs and outputs; convert any
    dict that uses this to a canonical one that doesn't, so that we
    have predictible input for the rest of the processing.

    Args:
        clt_desc (dict): The CWL CommandLineTool dict/list structure

    Returns:
        dict: A CWL CommandLineTool dict/list structure
    """
    if not 'inputs' in clt_desc:
        exit_validation("Error: no inputs defined for CommandLineTool")

    if isinstance(clt_desc['inputs'], dict):
        clt_desc['inputs'] = normalise_dict_list(clt_desc['inputs'])

    if not 'outputs' in clt_desc:
        exit_validation("Error: no outputs defined for CommandLineTool")

    if isinstance(clt_desc['outputs'], dict):
        clt_desc['outputs'] = normalise_dict_list(clt_desc['outputs'])


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
        if isinstance(input_value, dict):
            if 'class' not in input_value:
                exit_perm_fail('Error: missing class in input ' + input_name)
            if input_value['class'] == 'Directory':
                exit_system_error('Sorry: I don''t know how to deal with directories yet')
            if input_value['class'] == 'File':
                location = urlparse(input_value['location'])
                if 'basename' in input_value:
                    dest_path = os.path.join(workdir_path, input_value['basename'])
                else:
                    dest_path = os.path.join(workdir_path, os.path.basename(location.path))
                shutil.copy(location.path, dest_path)
                input_value['path'] = dest_path

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
    position = 100
    value = parameter.get('default')
    if 'id' not in parameter:
        exit_perm_fail("Error: input parameter given without an id")
    if 'type' in parameter:
        if parameter['type'] == 'File':
            if input_dict[parameter['id']]['class'] != 'File':
                exit_perm_fail('Error: expected File for input ' + parameter['id'])
            value = input_dict[parameter['id']]['path']
        else:
            value = input_dict.get(parameter['id'])

    if value is None:
        exit_perm_fail("Missing input for parameter " + parameter['id'])

    if 'inputBinding' in parameter:
        binding = parameter['inputBinding']
        if 'prefix' in binding:
            if 'separate' in binding:
                arg.append(binding['prefix'])
                arg.append(str(value))
            else:
                arg.append(binding['prefix'] + str(value))
        else:
            arg.append(str(value))

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
            if not instanceof(argument, str):
                exit_system_error('Sorry: I only understand strings for arguments.'
                        'Please use the inputs to pass arguments from input parameters.')
        args.append((-1, clt_desc['arguments']))

    for parameter in clt_desc['inputs']:
        args.append(create_argument(parameter, input_dict))

    args.sort(key=lambda arg: arg[0])

    # drop keys and flatten
    command_line = []
    for sort_key, items in args:
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

    stdin_file = None
    if in_out['stdin'] is not None:
        stdin_path = os.path.join(workdir_path, in_out['stdin'])
        stdin_file = open(stdin_path, 'rb')
    stdout_file = None
    if in_out['stdout'] is not None:
        stdout_path = os.path.join(workdir_path, in_out['stdout'])
        log("Writing stdout to " + stdout_path)
        stdout_file = open(stdout_path, 'wb')
    stderr_file = None
    if in_out['stderr'] is not None:
        stderr_path = os.path.join(workdir_path, in_out['stderr'])
        stderr_file = open(stderr_path, 'wb')

    log("Command line: " + str(command_line))
    result = subprocess.run(command_line,
            cwd=workdir_path,
            stdin=stdin_file,
            stdout=stdout_file,
            stderr=stderr_file)

    log("Ran subprocess")
    log("Result: " + str(result))

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
    moves (!) them to the current directory, so that the tempdir can
    be removed later without deleting the files.

    Args:
        output_dict (dict): A dict structure describing the output.

    Returns:
        dict: A dict structure describing the output in the new
                location.
    """
    for id, desc in output_dict.items():
        if isinstance(desc, dict):
            if desc['class'] == 'File':
                location = urlparse(desc['location'])
                dest_path = os.path.join(os.getcwd(), os.path.basename(location.path))
                shutil.move(location.path, dest_path)
                desc['location'] = 'file:///' + dest_path

    return output_dict

def run_command_line_tool(workdir_path, clt_dict, input_dict):
    """Executes a command line tool as described by a CommandLineTool
    object described in a CWL file (in JSON form).

    Args:
        clt_path (str): The path to the CWL file describing the tool
                to run.
        input_dict (dict): A dictionary describing inputs
    """
    normalise_command_line_tool(clt_dict)
    stage_input(workdir_path, input_dict)
    command_line = create_command_line(clt_dict, input_dict)
    base_command = clt_dict.get('baseCommand')
    in_out = {
            'stdin': clt_dict.get('stdin'),
            'stdout': clt_dict.get('stdout'),
            'stderr': clt_dict.get('stderr')
            }
    execute_clt(workdir_path, in_out, base_command, command_line)
    output_dict = collect_output(workdir_path, clt_dict['outputs'])
    return output_dict


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process a CWL workflow')
    parser.add_argument('cwlfile', type=str, help='A CWL file in JSON format')
    parser.add_argument('inputfile', type=str, help='An input file in JSON format')

    args = parser.parse_args()

    input_dict = json.load(open(args.inputfile, 'r'))
    clt_dict = json.load(open(args.cwlfile, 'r'))
    workdir_path = make_workdir()

    output_dict = run_command_line_tool(workdir_path, clt_dict, input_dict)
    output_dict = destage_output(output_dict)

    print(json.dumps(output_dict))

    remove_workdirs()
