#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: CommandLineTool
baseCommand: $CERISE_PROJECT_FILES/add_heading.sh
arguments: [$CERISE_PROJECT_FILES]

inputs:
  in_text:
    type: File
    inputBinding:
      position: 1

stdout: output.txt
outputs:
  out_text:
    type: File
    outputBinding: { glob: output.txt }
