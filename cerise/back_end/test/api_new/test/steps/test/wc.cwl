#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: CommandLineTool
baseCommand: wc

inputs:
  textfile:
    type: File
    inputBinding:
      position: 1

outputs:
  output:
    type: stdout
