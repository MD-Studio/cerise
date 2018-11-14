import cerise.back_end.cwl as cwl

from cerise.back_end.input_file import InputFile

def test_is_workflow():
    wf = """
        cwlVersion: v1.0
        class: Workflow

        inputs: []
        outputs: []

        steps: []
        """
    assert cwl.is_workflow(wf)


def test_is_not_workflow():
    wf = """
        cwlVersion: v1.0
        class: CommandLineTool
        baseCommand: hostname
        stdout: output.txt
        inputs: []

        outputs:
            output:
                type: File
                outputBinding: { glob: output.txt }
        """
    assert not cwl.is_workflow(wf)


def test_get_required_num_cores_coresmin():
    wf = """
        cwlVersion: v1.0
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            ResourceRequirement:
                coresMin: 10
        """
    assert cwl.get_required_num_cores(wf) == 10


def test_get_required_num_cores_coresmax():
    wf = """
        cwlVersion: v1.0
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            ResourceRequirement:
                coresMax: 10
        """
    assert cwl.get_required_num_cores(wf) == 10


def test_get_required_num_cores_both():
    wf = """
        cwlVersion: v1.0
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            ResourceRequirement:
                coresMin: 5
                coresMax: 10
        """
    assert cwl.get_required_num_cores(wf) == 5


def test_get_required_num_cores_default():
    wf = """
        cwlVersion: v1.0
        class: Workflow

        inputs: []
        outputs: []

        steps: []
        """
    assert cwl.get_required_num_cores(wf) == 0


def test_get_time_limit():
    wf = """
        cwlVersion: v1.1.0-dev1
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            TimeLimit: 123
        """
    assert cwl.get_time_limit(wf) == 123


def test_get_time_limit2():
    wf = """
        cwlVersion: v1.1.0-dev1
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            TimeLimit:
                timeLimit: 321
        """
    assert cwl.get_time_limit(wf) == 321


def test_no_time_limit():
    wf = """
        cwlVersion: v1.1.0-dev1
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            TimeLimit:
                timeLmt: 321
        """
    assert cwl.get_time_limit(wf) == 0


def test_no_time_limit2():
    wf = """
        cwlVersion: v1.1.0-dev1
        class: Workflow

        inputs: []
        outputs: []

        steps: []
        """
    assert cwl.get_time_limit(wf) == 0


def test_get_files_from_binding():
    binding = {
            "input_1": 10,
            "input_2": {
                "class": "File",
                "location": "http://example.com/test.txt"
                },
            "input_3": {
                "class": "File",
                "location": "http://example.com/test.in1",
                "secondaryFiles": [{
                    "class": "File",
                    "location": "http://example.com/test.in2"
                    }]
                },
            "input_4": [
                {
                    "class": "File",
                    "location": "http://example.com/test2.in1"
                },
                {
                    "class": "File",
                    "location": "http://example.com/test2.in2"
                }]
    }
    files = cwl.get_files_from_binding(binding)

    assert len(files) == 4

    for f in files:
        assert isinstance(f, InputFile)

    input_2 = [f for f in files if f.name == 'input_2'][0]
    assert input_2.index is None
    assert input_2.location == 'http://example.com/test.txt'
    assert input_2.content is None
    assert input_2.secondary_files == []


    input_3 = [f for f in files if f.name == 'input_3'][0]
    assert input_3.index is None
    assert input_3.location == 'http://example.com/test.in1'
    assert len(input_3.secondary_files) == 1
    assert input_3.secondary_files[0].location == 'http://example.com/test.in2'
    assert input_3.secondary_files[0].secondary_files == []

    input_4 = [f for f in files if f.name == 'input_4' and f.index == 0][0]
    assert input_4.location == 'http://example.com/test2.in1'
    input_4 = [f for f in files if f.name == 'input_4' and f.index == 1][0]
    assert input_4.location == 'http://example.com/test2.in2'
