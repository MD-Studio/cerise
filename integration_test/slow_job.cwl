#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

inputs:
  delay:
    type: float
    default: 4

outputs: []

steps:
  sleep:
    run: cerise/test/sleep.cwl
    in:
      delay: delay
    out: []
