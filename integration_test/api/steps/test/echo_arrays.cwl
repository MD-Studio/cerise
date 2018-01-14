#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: echo
stdout: output.txt
inputs:
  messages:
    type: string[]
    inputBinding:
        position: 1
  messages2:
    type: string[]
    inputBinding:
        prefix: -w
        separate: false
        position: 2
  numbers:
    type: int[]
    inputBinding:
        prefix: -x=
        itemSeparator: ','
        separate: false
        position: 3
  files:
    type: File[]
    inputBinding:
        prefix: -y
        itemSeparator: ','
        position: 4

outputs:
  output:
    type: File
    outputBinding: { glob: output.txt }
