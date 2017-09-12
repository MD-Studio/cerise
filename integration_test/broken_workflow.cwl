#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

inputs: []
outputs: []

steps:
  break:
    run: cerise/test/nonexisentt_step.cwl
    in:
      message: This is going to break
    out: []
