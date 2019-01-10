#!/bin/bash

echo "Testing API installation" >$CERISE_PROJECT_FILES/test_file.txt

# Cheat, we're not supposed to write outside of CERISE_PROJECT_FILES, but
# we need something that persists. So abuse the parent dir.
COUNT_FILE="$CERISE_PROJECT_FILES/../../count.txt"

if [ -e $COUNT_FILE ] ; then
    cur_count=$(cat $COUNT_FILE)
    new_count=$((cur_count + 1))
    echo $new_count >$COUNT_FILE
else
    echo '1' >$COUNT_FILE
fi
