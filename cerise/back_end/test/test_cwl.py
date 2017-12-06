import cerise.back_end.cwl as cwl

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
                }
    }
    files = cwl.get_files_from_binding(binding)

    assert len(files) == 2

    input_2 = [f for f in files if f[0] == 'input_2'][0]
    assert input_2 == ('input_2', 'http://example.com/test.txt', [])

    input_3 = [f for f in files if f[0] == 'input_3'][0]
    assert input_3[1] == 'http://example.com/test.in1'
    assert len(input_3[2]) == 1
    assert input_3[2][0].location == 'http://example.com/test.in2'
    assert input_3[2][0].secondary_files == []
