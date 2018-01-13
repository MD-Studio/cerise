from cerise.back_end.input_file import InputFile

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

    local_input = '{}'

    remote_input = '{}'

    remote_output = '{}\n'

    local_output = '{}'

class WcJob:
    """A simple job with an input file and an output file.
    """
    workflow = bytes(
                '#!/usr/bin/env cwl-runner\n'
                '\n'
                'cwlVersion: v1.0\n'
                'class: Workflow\n'
                'inputs:\n'
                '  file:\n'
                '    type: File\n'
                '\n'
                'outputs:\n'
                '  counts:\n'
                '    type: File\n'
                '    outputSource: wc/output\n'
                '\n'
                'steps:\n'
                '  wc:\n'
                '    run: test/wc.cwl\n'
                '    in:\n'
                '      textfile: file\n'
                '    out:\n'
                '      [output]\n', 'utf-8')

    def local_input(local_baseurl):
        return '{ "file": { "class": "File", "location": "' + local_baseurl + '/input/hello_world.txt" } }'

    local_input_files = [InputFile('file', 'input/hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8'), [])]

    remote_input = '{ "file": { "class": "File", "location": "work/01_input_hello_world.txt" } }'

    remote_input_files = [('file', '01_input_hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8'))]


    def remote_output(job_remote_workdir):
        return '{ "output": { "class": "File", "location": "' + job_remote_workdir + '/output.txt" } }\n'

    output_files = [
                ('output', 'output.txt', bytes(' 4 11 58 hello_world.txt', 'utf-8'))
                ]

    local_output = '{ "output": { "class": "File", "location": "output.txt" } }\n'


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

    def local_input(local_baseurl):
        return '{ "file": { "class": "File", "location": "' + local_baseurl + 'input/non_existing_file.txt" } }'

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

    local_input = '{}'

    remote_input = '{}'

class BrokenJob:
    """A simple job with no inputs or outputs, and an invalid command.
    """
    workflow = bytes(
                '#!/usr/bin/env cwl-runner\n'
                '\n'
                'cwlVersion: v1.0\n'
                'class: CommandLineTool\n'
                'baseCommand: this_comamnd_does_not_exist\n'
                'inputs: []\n'
                'outputs: []\n', 'utf-8')

    local_input = '{}'

    remote_input = '{}'

    output = ''


class SecondaryFilesJob:
    """A simple job with an input file with a secondary file and an output file.
    """
    workflow = bytes(
                '#!/usr/bin/env cwl-runner\n'
                '\n'
                'cwlVersion: v1.0\n'
                'class: Workflow\n'
                'inputs:\n'
                '  file:\n'
                '    type: File\n'
                '\n'
                'outputs:\n'
                '  counts:\n'
                '    type: File\n'
                '    outputSource: wc/output\n'
                '\n'
                'steps:\n'
                '  wc:\n'
                '    run: test/secondary_files.cwl\n'
                '    in:\n'
                '      textfile: file\n'
                '    out:\n'
                '      [output]\n', 'utf-8')

    def local_input(local_baseurl):
        return '''{{
            "file": {{
                "class": "File",
                "location": "{0}/input/hello_world.txt",
                "secondaryFiles": [{{
                    "class": "File",
                    "location": "{0}/input/hello_world.2nd"
                    }}]
                }}
            }}'''.format(local_baseurl)

    def local_input_files():
        input_file = InputFile('file', 'input/hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8'), [])
        input_file.secondary_files = [
            InputFile('file', 'input/hello_world.2nd', bytes(
                'Hello, secondaryFiles!', 'utf-8'), [])]
        return [input_file]

    remote_input = '''{
            "file": {
                "class": "File",
                "location": "work/01_input_hello_world.txt",
                "secondaryFiles": [{
                    "class": "File",
                    "location": "work/02_input_hello_world.2nd"
                    }]
                }
            }'''

    remote_input_files = [
            ('file', '01_input_hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8')),
            ('file', '02_input_hello_world.2nd', bytes(
                'Hello, secondaryFiles!', 'utf-8'))
            ]


