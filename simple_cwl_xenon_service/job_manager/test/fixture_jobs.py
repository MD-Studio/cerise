
class PassJob:
    """A simple job with no inputs or outputs.
    """
    workflow = bytes(
                '#!/usr/bin/env cwl-runner\n'
                '\n'
                'cwlVersion: v1.0\n'
                'class: CommandLineTool\n'
                'baseCommand: echo\n'
                'inputs: []\n'
                'outputs: []\n', 'utf-8')

    input = '{}'

    remote_input = '{}'

    output = '{}\n'

class WcJob:
    """A simple job with an input file and an output file.
    """
    workflow = bytes(
                '#!/usr/bin/env cwl-runner\n'
                '\n'
                'cwlVersion: v1.0\n'
                'class: CommandLineTool\n'
                'baseCommand: wc\n'
                'stdout: output.txt\n'
                'inputs:\n'
                '  file:\n'
                '    type: File\n'
                '    inputBinding:\n'
                '      position: 1\n'
                '\n'
                'outputs:\n'
                '  output:\n'
                '    type: File\n'
                '    outputBinding: { glob: output.txt }\n', 'utf-8')

    input = '{ "file": { "class": "File", "location": "input/hello_world.txt" } }'

    input_files = [('file', 'input/hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8'))]

    remote_input = '{ "file": { "class": "File", "location": "work/01_input_hello_world.txt" } }'

    remote_input_files = [('file', '01_input_hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8'))]


    def output(job_remote_workdir):
        return '{ "output": { "class": "File", "location": "' + job_remote_workdir + '/output.txt" } }\n'

    output_files = [
                ('output', 'output.txt', bytes(' 4 11 58 hello_world.txt', 'utf-8'))
                ]


class MissingInputJob:
    """A broken job that references an input file that doesn't exist.
    """
    workflow = bytes(
                '#!/usr/bin/env cwl-runner\n'
                '\n'
                'cwlVersion: v1.0\n'
                'class: CommandLineTool\n'
                'baseCommand: wc\n'
                'stdout: output.txt\n'
                'inputs:\n'
                '  file:\n'
                '    type: File\n'
                '    inputBinding:\n'
                '      position: 1\n'
                '\n'
                'outputs:\n'
                '  output:\n'
                '    type: File\n'
                '    outputBinding: { glob: output.txt }\n', 'utf-8')

    input = '{ "file": { "class": "File", "location": "input/non_existing_file.txt" } }'

    input_files = []

class SlowJob:
    workflow = bytes(
            '#!/usr/bin/env cwl-runner\n'
            '\n'
            'cwlVersion: v1.0\n'
            'class: CommandLineTool\n'
            'baseCommand: bash\n'
            'arguments:\n'
            '  - \'-c\'\n'
            '  - \'sleep 4\''
            '\n'
            'inputs: []\n'
            '\n'
            'stdout: output.txt\n'
            'outputs:\n'
            '  output:\n'
            '    type: File\n'
            '    outputBinding: { glob: output.txt }\n', 'utf-8')

    input = '{}'

    remote_input = '{}'

