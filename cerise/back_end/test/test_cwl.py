import pytest

import cerise.back_end.cwl as cwl
from cerise.back_end.file import File
from cerise.job_store.job_state import JobState


def test_is_workflow():
    wf = bytes("""
        cwlVersion: v1.0
        class: Workflow

        inputs: []
        outputs: []

        steps: []
        """, 'utf-8')
    assert cwl.is_workflow(wf)


def test_is_not_workflow():
    wf = bytes("""
        cwlVersion: v1.0
        class: CommandLineTool
        baseCommand: hostname
        stdout: output.txt
        inputs: []

        outputs:
            output:
                type: File
                outputBinding: { glob: output.txt }
        """, 'utf-8')
    assert not cwl.is_workflow(wf)


def test_is_workflow_invalid():
    workflow = b'$IA>$: [IIGAG'
    assert not cwl.is_workflow(workflow)


def test_is_workflow_required_attributes():
    wf = bytes("""
        cwlVersion: v1.0
        class: Workflow
        inputs: []
        outputs: []
        steps: []
        """, 'utf-8')
    assert cwl.is_workflow(wf)

    wf = bytes("""
        cwlVersion: v1.0
        inputs: []
        outputs: []
        steps: []
        """, 'utf-8')
    assert not cwl.is_workflow(wf)

    wf = bytes("""
        cwlVersion: v1.0
        class: Workflow
        outputs: []
        steps: []
        """, 'utf-8')
    assert not cwl.is_workflow(wf)

    wf = bytes("""
        cwlVersion: v1.0
        class: Workflow
        inputs: []
        steps: []
        """, 'utf-8')
    assert not cwl.is_workflow(wf)

    wf = bytes("""
        cwlVersion: v1.0
        class: Workflow
        inputs: []
        outputs: []
        """, 'utf-8')
    assert not cwl.is_workflow(wf)


def test_get_workflow_step_names_1():
    workflow = bytes(
            'cwlVersion: v1.0\n'
            'class: Workflow\n'
            '\n'
            'inputs: []\n'
            'outputs: []\n'
            '\n'
            'steps:\n'
            '  step1:\n'
            '    run: test/test.cwl\n'
            '  step2:\n'
            '    run: test/test2.cwl\n', 'utf-8')

    names = cwl.get_workflow_step_names(workflow)
    assert len(names) == 2
    assert 'test/test.cwl' in names
    assert 'test/test2.cwl' in names


def test_get_workflow_step_names_2():
    workflow = bytes(
            'cwlVersion: v1.0\n'
            'class: Workflow\n'
            '\n'
            'inputs: []\n'
            'outputs: []\n'
            '\n'
            'steps:\n'
            '  - id: step1\n'
            '    run: test/test.cwl\n'
            '  - id: step2\n'
            '    run: test/test2.cwl\n', 'utf-8')

    names = cwl.get_workflow_step_names(workflow)
    assert names == ['test/test.cwl', 'test/test2.cwl']


def test_get_workflow_step_names_3():
    workflow = bytes(
            'cwlVersion: v1.0\n'
            'class: Workflow\n'
            '\n'
            'inputs: []\n'
            'outputs: []\n'
            '\n'
            'steps: []\n', 'utf-8')

    names = cwl.get_workflow_step_names(workflow)
    assert names == []


def test_get_workflow_step_names_4():
    workflow = bytes(
            'cwlVersion: v1.0\n'
            'class: Workflow\n'
            '\n'
            'inputs: []\n'
            'outputs: []\n'
            '\n'
            'steps: 13\n', 'utf-8')

    with pytest.raises(RuntimeError):
        cwl.get_workflow_step_names(workflow)


def test_get_required_num_cores_coresmin():
    wf = bytes("""
        cwlVersion: v1.0
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            ResourceRequirement:
                coresMin: 10
        """, 'utf-8')
    assert cwl.get_required_num_cores(wf) == 10


def test_get_required_num_cores_coresmax():
    wf = bytes("""
        cwlVersion: v1.0
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            ResourceRequirement:
                coresMax: 10
        """, 'utf-8')
    assert cwl.get_required_num_cores(wf) == 10


def test_get_required_num_cores_both():
    wf = bytes("""
        cwlVersion: v1.0
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            ResourceRequirement:
                coresMin: 5
                coresMax: 10
        """, 'utf-8')
    assert cwl.get_required_num_cores(wf) == 5


def test_get_required_num_cores_default():
    wf = bytes("""
        cwlVersion: v1.0
        class: Workflow

        inputs: []
        outputs: []

        steps: []
        """, 'utf-8')
    assert cwl.get_required_num_cores(wf) == 0


def test_get_time_limit():
    wf = bytes("""
        cwlVersion: v1.1.0-dev1
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            TimeLimit: 123
        """, 'utf-8')
    assert cwl.get_time_limit(wf) == 123


def test_get_time_limit2():
    wf = bytes("""
        cwlVersion: v1.1.0-dev1
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            TimeLimit:
                timeLimit: 321
        """, 'utf-8')
    assert cwl.get_time_limit(wf) == 321


def test_no_time_limit():
    wf = bytes("""
        cwlVersion: v1.1.0-dev1
        class: Workflow

        inputs: []
        outputs: []

        steps: []

        hints:
            TimeLimit:
                timeLmt: 321
        """, 'utf-8')
    with pytest.raises(ValueError):
        cwl.get_time_limit(wf)


def test_no_time_limit2():
    wf = bytes("""
        cwlVersion: v1.1.0-dev1
        class: Workflow

        inputs: []
        outputs: []

        steps: []
        """, 'utf-8')
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
                    "location": "http://example.com/test.in2",
                    "secondaryFiles": [{
                        "class": "File",
                        "location": "http://example.com/test.in2.in3"
                        }]
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
        assert isinstance(f, File)

    input_2 = [f for f in files if f.name == 'input_2'][0]
    assert input_2.index is None
    assert input_2.location == 'http://example.com/test.txt'
    assert input_2.source is None
    assert input_2.secondary_files == []


    input_3 = [f for f in files if f.name == 'input_3'][0]
    assert input_3.index is None
    assert input_3.location == 'http://example.com/test.in1'
    assert len(input_3.secondary_files) == 1
    sf0 = input_3.secondary_files[0]
    assert sf0.location == 'http://example.com/test.in2'
    assert len(sf0.secondary_files) == 1
    sf1 = sf0.secondary_files[0]
    assert sf1.location == 'http://example.com/test.in2.in3'

    input_4 = [f for f in files if f.name == 'input_4' and f.index == 0][0]
    assert input_4.location == 'http://example.com/test2.in1'
    input_4 = [f for f in files if f.name == 'input_4' and f.index == 1][0]
    assert input_4.location == 'http://example.com/test2.in2'


def test_get_files_from_binding_directory_1():
    binding = {
            "input_1": {
                "class": "Directory",
                "location": "http://example.com/test"
                }
    }
    # This is valid, but not yet supported
    with pytest.raises(RuntimeError):
        cwl.get_files_from_binding(binding)


def test_get_files_from_binding_directory_2():
    binding = {
            "input_1": {
                "class": "File",
                "location": "http://example.com/test.idx",
                "secondaryFiles": [{
                    "class": "Directory",
                    "location": "http://example.com/test"}]
                }
    }
    # This is valid, but not yet supported
    with pytest.raises(RuntimeError):
        cwl.get_files_from_binding(binding)


def test_get_cwltool_result():
    result = ('Stuff\n'
              'Tool definition failed validation:\n'
              'Bla\n')
    assert cwl.get_cwltool_result(result) == JobState.PERMANENT_FAILURE

    result = ('Stuff\n'
              'Final process status is permanentFail\n'
              'Bla\n')
    assert cwl.get_cwltool_result(result) == JobState.PERMANENT_FAILURE

    result = ('Stuff\n'
              'Final process status is temporaryFail\n'
              'Bla\n')
    assert cwl.get_cwltool_result(result) == JobState.TEMPORARY_FAILURE

    result = ('Stuff\n'
              'Final process status is success\n'
              'Bla\n')
    assert cwl.get_cwltool_result(result) == JobState.SUCCESS

    result = 'Stuff success Bla'
    assert cwl.get_cwltool_result(result) == JobState.SYSTEM_ERROR

    result = 'StuffpermanentFailBla'
    assert cwl.get_cwltool_result(result) == JobState.SYSTEM_ERROR
