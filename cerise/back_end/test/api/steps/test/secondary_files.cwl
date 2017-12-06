#!/usr/bin/env cwl-runner

cwlVersion: v1.0

class: CommandLineTool
baseCommand: bash
arguments: ["-c", "wc *"]

inputs:
  textfile:
    type: File

outputs:
  output:
    type: stdout
