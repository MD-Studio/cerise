#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: Workflow

inputs:
  file: File

outputs:
  output:
    type: File
    outputSource: wc/output

steps:
  wc:
    run: test/wc_all.cwl
    in:
      file: file
    out:
      - output
