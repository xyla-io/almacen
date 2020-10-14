#!/bin/bash

SCRIPT_DIR=$( cd "$( dirname "$0" )" && pwd )
# This regex pattern should be sanitized in case the directory path contains special regex characters.
SCRIPT_PROCESS=$(ps -ax | grep "${SCRIPT_DIR}/almacen\.py")
if [[ ! -z "$SCRIPT_PROCESS" ]]; then
  echo "almacen.py script already running as process $SCRIPT_PROCESS"
  exit 1
fi
source "${SCRIPT_DIR}/venv/bin/activate"
nohup python "${SCRIPT_DIR}/almacen.py" "$@" &