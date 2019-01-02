#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: CommandLineTool
baseCommand: cat
arguments: [$CERISE_PROJECT_FILES/test_file.txt]

inputs: []

stdout: output.txt
outputs:
  output:
    type: File
    outputBinding: { glob: output.txt }
