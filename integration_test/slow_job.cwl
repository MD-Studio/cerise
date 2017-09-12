#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

inputs: []
outputs: []

steps:
  sleep:
    run: cerise/test/sleep.cwl
    in:
      delay:
        default: 4
    out: []
