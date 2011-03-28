#!/bin/bash

# This script 'curries' any command to make it run under the given virtualenv.
# Used when commands are run by an outside script.

if [ "X$1" = "X" ]; then
    echo "expecting argument #1: virtualenv activate file" 1>&2 
    exit 1
fi

if [ "X$2" = "X" ]; then
    echo "expecting argument #2 - N: script and args to run" 1>&2
    exit 2
fi

source $1
if [ $? -ne 0 ]; then
    echo "Problem sourcing the environment file: $1" 1>&2
    exit 3
fi

shift 1

$@
