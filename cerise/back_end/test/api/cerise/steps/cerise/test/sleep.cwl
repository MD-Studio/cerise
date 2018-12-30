cwlVersion: v1.0

class: CommandLineTool
baseCommand: sleep
requirements:
  - class: ShellCommandRequirement

inputs:
  delay:
    type: int
    inputBinding:
      position: 1

outputs: []

