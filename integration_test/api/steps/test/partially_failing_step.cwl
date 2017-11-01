#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: CommandLineTool
baseCommand: $CERISE_API_FILES/test/partially_failing_program.sh

inputs: []

stdout: output.txt
outputs:
  output:
    type: File
    outputBinding: { glob: output.txt }
  missing_output:
    type: File
    outputBinding: { glob: missing_output.txt }
