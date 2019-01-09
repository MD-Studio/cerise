from cerise.back_end.file import File


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

    input_content = {}

    required_num_cores = 0

    time_limit = 0

    def remote_input(job_remote_workdir):
        return {}

    remote_input_files = []

    def remote_output(job_remote_workdir):
        """Argument is remote work dir for this job.
        """
        return '{}\n'

    output_files = []

    output_content = {}

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
                '    run: test/hostname.cwl\n'
                '    out:\n'
                '      [output]\n'
                'hints:\n'
                '  TimeLimit: 101\n', 'utf-8')

    def local_input(local_baseurl):
        return '{}'

    local_input_files = []

    input_content = {}

    required_num_cores = 2

    time_limit = 101

    def remote_input(job_remote_workdir):
        return {}

    remote_input_files = []

    def remote_output(job_remote_workdir):
        return ('{{ "host": {{ "class": "File", "location": "{}/output.txt" }}'
                ' }}\n').format(job_remote_workdir)

    output_files = [File('host', None, 'output.txt', [])]

    output_content = {'output.txt': b'hostname\n'}

    local_output = ('{ "host": { "class": "File", "location": "output.txt" }'
                    '}\n')


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
                '      file: file\n'
                '    out:\n'
                '      [output]\n'
                '\n'
                'hints:\n'
                '  ResourceRequirement:\n'
                '    coresMin: 3\n', 'utf-8')

    def local_input(local_baseurl):
        return ('{ "file": { "class": "File", "location":'
                '"%shello_world.txt" } }') % local_baseurl

    local_input_files = [File('file', None, 'hello_world.txt', [])]

    input_content = {
            'hello_world.txt': bytes(
                    'Hello, World!\n'
                    '\n'
                    'Here is a test file for the staging test.\n'
                    '\n', 'utf-8')}

    required_num_cores = 3

    time_limit = 60

    def remote_input(job_remote_workdir):
        return {
                'file': {
                    'class': 'File',
                    'location': '{}/01_hello_world.txt'.format(
                        job_remote_workdir)
                }
            }

    remote_input_files = [('file', '01_hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8'))]

    def remote_output(job_remote_workdir):
        return ('{{ "output": {{ "class": "File", "location": "{}/output.txt"'
                ' }} }}\n').format(job_remote_workdir)

    output_files = [
            File('output', None, 'output.txt', [])]

    output_content = {'output.txt': b' 4 11 58 hello_world.txt'}

    local_output = ('{ "output": { "class": "File", "location": "output.txt"'
                    ' } }\n')


class SlowJob:
    workflow = bytes(
            '#!/usr/bin/env cwl-runner\n'
            '\n'
            'cwlVersion: v1.0\n'
            'class: Workflow\n'
            'steps:\n'
            '  sleep:\n'
            '    run: test/sleep.cwl\n'
            '    in:\n'
            '      delay:\n'
            '        default: 1\n'
            '\n'
            'inputs: []\n'
            '\n'
            'outputs: []\n', 'utf-8')

    def local_input(local_baseurl):
        return '{}'

    local_input_files = []

    input_content = {}

    required_num_cores = 0

    time_limit = 0

    def remote_input(job_remote_workdir):
        return {}

    remote_input_files = []

    def remote_output(job_remote_workdir):
        return '{}'

    output_files = []

    output_content = {}

    local_output = '{}'


class SecondaryFilesJob:
    """A simple job with an input file with a secondary file.
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
                "location": "{0}hello_world.txt",
                "secondaryFiles": [{{
                    "class": "File",
                    "location": "{0}hello_world.2nd"
                    }}]
                }}
            }}'''.format(local_baseurl)

    def _make_local_input_files():
        input_file = File('file', None, 'hello_world.txt', [])
        input_file.secondary_files = [File(None, None, 'hello_world.2nd', [])]
        return [input_file]

    local_input_files = _make_local_input_files()

    input_content = {
            'hello_world.txt': bytes(
                    'Hello, World!\n'
                    '\n'
                    'Here is a test file for the staging test.\n'
                    '\n', 'utf-8'),
            'hello_world.2nd': b'Hello, secondaryFiles!'}

    required_num_cores = 0

    time_limit = 0

    def remote_input(job_remote_workdir):
        return {
                'file': {
                    'class': 'File',
                    'location': '{}/01_hello_world.txt'.format(
                        job_remote_workdir),
                    'secondaryFiles': [{
                        'class': 'File',
                        'location': '{}/02_hello_world.2nd'.format(
                            job_remote_workdir),
                    }]
                }
            }

    remote_input_files = [
            ('file', '01_hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8')),
            ('file', '02_hello_world.2nd', bytes(
                'Hello, secondaryFiles!', 'utf-8'))
            ]

    def remote_output(job_remote_workdir):
        return ('{{ "counts": {{ "class": "File", "location": "{}/output.txt"'
                ' }} }}\n').format(job_remote_workdir)

    output_files = [File('counts', None, 'output.txt', [])]

    output_content = {'output.txt': b' 4 11 58 hello_world.txt'}

    local_output = ('{ "counts": { "class": "File", "location": "output.txt"'
                    ' } }\n')


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
            '    run: test/file_array.cwl\n'
            '    in:\n'
            '      files: files\n'
            '    out:\n'
            '      [output]\n', 'utf-8')

    def local_input(local_baseurl):
        return '''{{
            "files": [
                {{
                    "class": "File",
                    "location": "{0}hello_world.txt"
                    }},
                {{
                    "class": "File",
                    "location": "{0}hello_world.2nd"
                }}]
            }}'''.format(local_baseurl)

    def _make_local_input_files():
        input_file_1 = File('files', 0, 'hello_world.txt', [])
        input_file_2 = File('files', 1, 'hello_world.2nd', [])
        return [input_file_1, input_file_2]

    local_input_files = _make_local_input_files()

    input_content = {
            'hello_world.txt': bytes(
                    'Hello, World!\n'
                    '\n'
                    'Here is a test file for the staging test.\n'
                    '\n', 'utf-8'),
            'hello_world.2nd': b'Hello, file arrays!'}

    required_num_cores = 0

    time_limit = 60

    def remote_input(job_remote_workdir):
        return {
                'files': [{
                        'class': 'File',
                        'location': '{}/01_hello_world.txt'.format(
                            job_remote_workdir)
                    },
                    {
                        'class': 'File',
                        'location': '{}/02_hello_world.2nd'.format(
                            job_remote_workdir)
                    }]
            }

    remote_input_files = [
            ('files', '01_hello_world.txt', bytes(
                'Hello, World!\n'
                '\n'
                'Here is a test file for the staging test.\n'
                '\n', 'utf-8')),
            ('files', '02_hello_world.2nd', bytes(
                'Hello, file arrays!', 'utf-8'))
            ]

    def remote_output(job_remote_workdir):
        return ('{{ "counts": {{ "class": "File", "location": "{}/output.txt"'
                ' }} }}\n').format(job_remote_workdir)

    output_files = [File('counts', None, 'output.txt', [])]

    output_content = {'output.txt': b' 4 11 58 hello_world.txt'}

    local_output = ('{{ "counts": {{ "class": "File", "location": "output.txt"'
                    ' }} }}\n')


class LongRunningJob:
    workflow = bytes(
            '#!/usr/bin/env cwl-runner\n'
            '\n'
            'cwlVersion: v1.0\n'
            'class: Workflow\n'
            'steps:\n'
            '  sleep:\n'
            '    run: test/sleep.cwl\n'
            '    in:\n'
            '      delay: 120\n'
            '\n'
            'inputs: []\n'
            '\n'
            'outputs: []\n', 'utf-8')

    def local_input(local_baseurl):
        return '{}'

    local_input_files = []

    required_num_cores = 0

    time_limit = 0

    output_files = []

    output_content = {}

    local_output = '{}\n'


class InstallScriptTestJob:
    workflow = bytes(
            '#!/usr/bin/env cwl-runner\n'
            '\n'
            'cwlVersion: v1.0\n'
            'class: Workflow\n'
            'steps:\n'
            '  test_install:\n'
            '    run: test/test_install_script.cwl\n'
            '    out: [output]\n'
            '\n'
            'inputs: []\n'
            '\n'
            'outputs:\n'
            '  output:\n'
            '    type: File\n'
            '    outputSource: test_install/output\n'
            '\n', 'utf-8')

    def local_input(local_baseurl):
        return '{}'

    local_input_files = []

    input_content = {}

    required_num_cores = 0

    time_limit = 0

    def remote_output(job_remote_workdir):
        return ('{{ "output": {{ "class": "File", "location": "{}/output.txt"'
                ' }} }}\n').format(job_remote_workdir)

    output_files = [File('host', None, 'output.txt', [])]

    output_content = [('output.txt', b'Testing API installation\n')]

    local_output = ('{ "host": { "class": "File", "location": "output.txt"'
                    ' } }\n')


class PartiallyFailingJob:
    workflow = bytes(
                '#!/usr/bin/env cwl-runner\n'
                '\n'
                'cwlVersion: v1.0\n'
                'class: Workflow\n'
                'inputs: []\n'
                'outputs:\n'
                '  output:\n'
                '    type: File\n'
                '    outputSource: failing/output\n'
                '  missing_output:\n'
                '    type: File\n'
                '    outputSource: failing/missing_output\n'
                '\n'
                'steps:\n'
                '  failing:\n'
                '    run: test/partially_failing_step.cwl\n'
                '    in: []\n'
                '    out:\n'
                '      [output, missing_output]\n', 'utf-8')

    def local_input(local_baseurl):
        return '{}'

    local_input_files = []

    input_content = {}

    required_num_cores = 0

    time_limit = 0

    def remote_input(job_remote_workdir):
        return {}

    remote_input_files = []

    def remote_output(job_remote_workdir):
        return ('{{ "output": {{ "class": "File",'
                ' "location": "{}/output.txt" }},\n'
                '   "missing_output": null }}\n').format(job_remote_workdir)

    output_files = [File('output', None, 'output.txt', [])]

    output_content = [('output.txt', b'Running on host: hostname\n')]

    local_output = ('{ "output": { "class": "File", "location": "output.txt"'
                    ' }, "missing_output": null }\n')


class NoSuchStepJob:
    workflow = bytes(
            '#!/usr/bin/env cwl-runner\n'
            '\n'
            'cwlVersion: v1.0\n'
            'class: Workflow\n'
            'steps:\n'
            '  sleep:\n'
            '    run: test/no_such_step.cwl\n'
            '    in:\n'
            '      delay: 120\n'
            '\n'
            'inputs: []\n'
            '\n'
            'outputs: []\n', 'utf-8')

    def local_input(local_baseurl):
        return '{}'

    local_input_files = []

    input_content = {}

    required_num_cores = 0

    time_limit = 0

    def remote_input(job_remote_workdir):
        return {}

    remote_input_files = []

    def remote_output(job_remote_workdir):
        return ''

    output_files = []

    output_content = {}

    local_output = '{}\n'


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
        return ('{{ "file": {{ "class": "File", "location":'
                ' "{}non_existing_file.txt" }} }}').format(local_baseurl)

    local_input_files = [File('file', None, 'non_existing_file.txt', [])]

    input_content = {}

    def remote_input(job_remote_workdir):
        return {
                'file': {
                    'class': 'File',
                    'location': '{}/01_non_existing_file.txt'.format(
                        job_remote_workdir)
                }
            }

    remote_input_files = []


class BrokenJob:
    """A simple job with no inputs or outputs, and an invalid command.
    And an invalid scheme in the input description.
    """
    workflow = bytes(
                '#!/usr/bin/env cwl-runner\n'
                '\n'
                'cwlVersion: v1.0\n'
                'class: CommandLineTool\n'
                'baseCommand: this_comamnd_does_not_exist\n'
                'inputs: []\n'
                'outputs: []\n', 'utf-8')

    def local_input(local_base_url):
        return ('{ "file": { "class": "File", "location":'
                '"does_not_exist://hello_world.txt" } }')

    local_input_files = []

    input_content = {}

    required_num_cores = 0

    time_limit = 0

    def remote_input(job_remote_workdir):
        return {}

    remote_input_files = []

    def remote_output(job_remote_workdir):
        return ''

    output_files = []

    output_content = {}


class NoWorkflowJob:
    """A job without a workflow.
    """
    workflow = None

    def local_input(local_base_url):
        return '{}'

    local_input_files = []

    input_content = {}

    required_num_cores = 0

    time_limit = 0

    def remote_input(job_remote_workdir):
        return {}

    remote_input_files = []

    def remote_output(job_remote_workdir):
        return ''

    output_files = []

    output_content = {}
