#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: Workflow

inputs: []

outputs:
  output:
    type: File
    outputSource: test_install_script/output

steps:
  test_install_script:
    run: test/test_install_script.cwl
    in: []
    out:
      - output
