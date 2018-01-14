#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: Workflow

inputs:
  messages:
    type: string[]
  messages2:
    type: string[]
    prefix: -w
    separate: false
  numbers:
    type: int[]
    prefix: -x=
    itemSeparator: ','
    separate: false
  files:
    type: File[]
    prefix: -y
    itemSeparator: ','

outputs:
  output:
    type: File
    outputSource: echo/output

steps:
  echo:
    run: test/echo_arrays.cwl
    in:
        messages: messages
        messages2: messages2
        numbers: numbers
        files: files
    out:
      - output
