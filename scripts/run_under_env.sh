#!/bin/bash
# Copyright 2013 University of Chicago


# This script runs a command under the given environment file.
# Used when commands are run by an outside script (intended for virtualenv but generic enough).

if [ "X$1" = "X" ]; then
    echo "expecting argument #1: environment file" 1>&2
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
