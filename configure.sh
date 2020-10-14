#!/bin/bash

set -e

cd $(dirname $0)

DEFAULT_CONFIG='{
  "enable_tagging": true
}'

if [ ! -f configure.json ]; then
  echo "$DEFAULT_CONFIG" > configure.json
fi
if [ "$1" == '-i' ]; then
  echo "# Edit the configuration file. Lines starting with # will be removed. The default configuration is below for reference." >> configure.json
  echo "$DEFAULT_CONFIG" | sed 's/^/#/' >> configure.json
  vi configure.json
  CLEANED_CONFIG=$(cat configure.json | sed '/^#/ d')
  echo "$CLEANED_CONFIG" > configure.json
fi
cat configure.json
