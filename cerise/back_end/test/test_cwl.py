import cerise.back_end.cwl as cwl
from cerise.job_store.job_state import JobState

import os
import pytest

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
            }
    }
    files = cwl.get_files_from_binding(binding)

    assert files == [('input_2', 'http://example.com/test.txt')]
