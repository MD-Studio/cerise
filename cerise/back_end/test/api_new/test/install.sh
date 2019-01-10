#!/bin/bash

CERISE_API_FILES="$1"

mkdir -p $CERISE_API_FILES/test
echo "Testing API installation at $CERISE_API_FILES" >$CERISE_API_FILES/test/test_file2.txt
