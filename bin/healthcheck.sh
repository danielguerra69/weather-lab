#!/bin/bash

# Check if the main process is running
if pgrep -f "python src/main.py" > /dev/null
then
  echo "Weather Lab is running"
  exit 0
else
  echo "Weather Lab is not running"
  exit 1
fi
