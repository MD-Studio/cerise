#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: bash
arguments:
  - '-c'
  - 'sleep 4'

inputs: []

stdout: output.txt
outputs:
  output:
    type: File
    outputBinding: { glob: output.txt }
