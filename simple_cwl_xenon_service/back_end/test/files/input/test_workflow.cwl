#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: hostname
stdout: output.txt
inputs: []

outputs:
  output:
    type: File
    outputBinding: { glob: output.txt }
