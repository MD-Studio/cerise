#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: $CERISE_API_FILES/wc_all.sh
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
