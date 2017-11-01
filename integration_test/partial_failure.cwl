#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: Workflow

inputs: []

outputs:
  output:
    type: File
    outputSource: failing/output
  missing_output:
    type: File
    outputSource: failing/missing_output

steps:
  failing:
    run: test/partially_failing_step.cwl
    in: []
    out:
      - output
      - missing_output
