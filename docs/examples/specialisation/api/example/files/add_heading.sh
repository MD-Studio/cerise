#!/bin/bash

CERISE_PROJECT_FILES="$1"
INPUT_FILE="$2"

cat $CERISE_PROJECT_FILES/heading.txt $INPUT_FILE
