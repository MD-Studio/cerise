#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: Workflow

inputs:
  input_file:
    type: File

outputs:
  output:
    type: File
    outputSource: custom/out_text

steps:
  custom:
    run: example/custom_program.cwl
    in:
      in_text: input_file
    out:
      - out_text
