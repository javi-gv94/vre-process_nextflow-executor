#!/bin/bash

###
### Testing in a local installation
### the VRE server CMD
###
### * Automatically created by MuGVRE *
###
REALPATH="$(realpath "$0")"
BASEDIR="$(dirname "$REALPATH")"
case "$BASEDIR" in
	/*)
		true
		;;
	*)
		BASEDIR="${PWD}/$BASEDIR"
		;;
esac

TOOL_EXECUTABLE="${BASEDIR}/VRE_NF_RUNNER"
TEST_DATA_DIR="${BASEDIR}/tests/json/naive_workflow"

# The relative directory does matter!
cd "$BASEDIR"
time "$TOOL_EXECUTABLE" --config "$TEST_DATA_DIR"/config.json --in_metadata "$TEST_DATA_DIR"/in_metadata.json --out_metadata "$BASEDIR"/out_metadata.json --log_file "$BASEDIR"/tool.log
