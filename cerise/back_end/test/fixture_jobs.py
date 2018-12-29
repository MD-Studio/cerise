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

    def local_input(local_baseurl):
        """Argument is local input dir for this job.

        That's normally local_exchange / input / job_name.
        """
        return '{}'

    local_input_files = []

    remote_input = '{}'

    remote_input_files = []

    def remote_output(job_remote_workdir):
        """Argument is remote work dir for this job.
        """
        return '{}\n'

    output_files = []

    local_output = '{}'


class HostnameJob:
    """A simple job with no inputs and one output.
    """
    workflow = bytes(
                '#!/usr/bin/env cwl-runner\n'
                '\n'
                'cwlVersion: v1.0\n'
                'class: Workflow\n'
                'inputs: []\n'
                'outputs:\n'
                '  host:\n'
                '    type: File\n'
                '    outputSource: hostname/output\n'
                '\n'
                'steps:\n'
                '  hostname:\n'
                '    run: cerise/test/hostname.cwl\n'
                '    out:\n'
                '      [output]\n'
                'hints:\n'
                '  TimeLimit: 101\n', 'utf-8')

    def local_input(local_baseurl):
        return '{}'

    local_input_files = []

    remote_input = '{}'

    remote_input_files = []

    def remote_output(job_remote_workdir):
        return '{ "host": { "class": "File", "location:" "{}/output.txt" } }\n'.format(
                job_remote_workdir)

    output_files = [
            ('host', 'output.txt', bytes('hostname', 'utf-8'))]

    local_output = '{ "host": { "class": "File", "location": "output.txt" } }\n'


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
                '    run: cerise/test/wc.cwl\n'
                '    in:\n'
                '      textfile: file\n'
                '    out:\n'
                '      [output]\n'
                '\n'
                'hints:\n'
                '  ResourceRequirement:\n'
                '    coresMin: 3\n', 'utf-8')

    def local_input(local_baseurl):
        return ('{ "file": { "class": "File", "location":'
                '"%sinput/test_job/hello_world.txt" } }') % local_baseurl

    local_input_files = [InputFile('file', 'input/test_job/hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8'), [])]

    remote_input = '{ "file": { "class": "File", "location": "work/01_input_test_job_hello_world.txt" } }'

    remote_input_files = [('file', '01_input_test_job_hello_world.txt', bytes(
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

    def local_input(local_baseurl):
        return '{}'

    local_input_files = []

    remote_input = '{}'

    remote_input_files = []

    def remote_output(job_remote_workdir):
        return '{ "output": { "class": "File", "location": "{}/output.txt" } }\n'.format(job_remote_workdir)

    output_files = [
            ('output', 'output.txt', bytes('', 'utf-8'))]

    local_output = '{ "output": { "class": "File", "location": "output.txt" } }\n'


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
                '    run: cerise/test/secondary_files.cwl\n'
                '    in:\n'
                '      textfile: file\n'
                '    out:\n'
                '      [output]\n', 'utf-8')

    def local_input(local_baseurl):
        return '''{{
            "file": {{
                "class": "File",
                "location": "{0}input/test_job/hello_world.txt",
                "secondaryFiles": [{{
                    "class": "File",
                    "location": "{0}input/test_job/hello_world.2nd"
                    }}]
                }}
            }}'''.format(local_baseurl)

    def _make_local_input_files():
        input_file = InputFile('file', 'input/test_job/hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8'), [])
        input_file.secondary_files = [
            InputFile(None, 'input/test_job/hello_world.2nd', bytes(
                'Hello, secondaryFiles!', 'utf-8'), [])]
        return [input_file]

    local_input_files = _make_local_input_files()

    remote_input = '''{
            "file": {
                "class": "File",
                "location": "work/01_input_test_job_hello_world.txt",
                "secondaryFiles": [{
                    "class": "File",
                    "location": "work/02_input_test_job_hello_world.2nd"
                    }]
                }
            }'''

    remote_input_files = [
            ('file', '01_input_test_job_hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8')),
            ('file', '02_input_test_job_hello_world.2nd', bytes(
                'Hello, secondaryFiles!', 'utf-8'))
            ]

    def remote_output(job_remote_workdir):
        return '{ "counts": { "class": "File", "location": "{}/output.txt" } }\n'.format(job_remote_workdir)

    output_files = [
                ('counts', 'output.txt', bytes(' 4 11 58 hello_world.txt', 'utf-8'))
                ]

    local_output = '{ "counts": { "class": "File", "location": "output.txt" } }\n'


class FileArrayJob:
    """A simple job with an array of input files.
    """
    workflow = bytes(
                '#!/usr/bin/env cwl-runner\n'
                '\n'
                'cwlVersion: v1.0\n'
                'class: Workflow\n'
                'inputs:\n'
                '  files:\n'
                '    type: File[]\n'
                '\n'
                'outputs:\n'
                '  counts:\n'
                '    type: File\n'
                '    outputSource: wc/output\n'
                '\n'
                'steps:\n'
                '  wc:\n'
                '    run: cerise/test/file_array.cwl\n'
                '    in:\n'
                '      textfiles: files\n'
                '    out:\n'
                '      [output]\n', 'utf-8')

    def local_input(local_baseurl):
        return '''{{
            "files": [
                {{
                    "class": "File",
                    "location": "{0}input/test_job/hello_world.txt"
                    }},
                {{
                    "class": "File",
                    "location": "{0}input/test_job/hello_world.2nd"
                }}]
            }}'''.format(local_baseurl)

    def _make_local_input_files():
        input_file_1 = InputFile('files', 'input/test_job/hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8'), [], 0)
        input_file_2 = InputFile('files', 'input/test_job/hello_world.2nd', bytes(
                'Hello, file arrays!', 'utf-8'), [], 1)
        return [input_file_1, input_file_2]

    local_input_files = _make_local_input_files()

    remote_input = '''{
            "files": [{
                    "class": "File",
                    "location": "work/01_input_test_job_hello_world.txt"
                },
                {
                    "class": "File",
                    "location": "work/02_input_test_job_hello_world.2nd"
                }]
            }'''

    remote_input_files = [
            ('files', '01_input_test_job_hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8')),
            ('files', '02_input_test_job_hello_world.2nd', bytes(
                'Hello, file arrays!', 'utf-8'))
            ]

    def remote_output(job_remote_workdir):
        return '{ "counts": { "class": "File", "location": "{}/output.txt" } }\n'.format(job_remote_workdir)

    output_files = [
                ('counts', 'output.txt', bytes(' 4 11 58 hello_world.txt', 'utf-8'))
                ]

    local_output = '{ "counts": { "class": "File", "location": "output.txt" } }\n'


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
        return '{ "file": { "class": "File", "location": "' + local_baseurl + 'non_existing_file.txt" } }'

    input_files = []


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
