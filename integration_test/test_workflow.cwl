#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: Workflow

inputs:
  message: string

outputs:
  output:
    type: File
    outputSource: echo/output

steps:
  echo:
    run: cerise/test/echo.cwl
    in:
        message: message
    out:
      - output
