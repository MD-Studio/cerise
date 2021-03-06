#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: Workflow

inputs: []

outputs:
  output:
    type: File
    outputSource: hostname/output

steps:
  hostname:
    run: example/hostname.cwl
    in: []
    out:
      - output
