#!/bin/bash

BASENAME=`basename $1 .txt`

wc hello_world.txt ${BASENAME}.2nd
