#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: wc
stdout: output.txt
inputs:
  file:
    type: File
    inputBinding:
      position: 1

outputs:
  output:
    type: File
    outputBinding: { glob: output.txt }
