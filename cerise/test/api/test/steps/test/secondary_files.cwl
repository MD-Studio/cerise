#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: CommandLineTool
baseCommand: bash
arguments: ["-c", "wc *"]
stdout: output.txt

inputs:
  textfile:
    type: File

outputs:
  output:
    type: File
    outputBinding: { glob: output.txt }
