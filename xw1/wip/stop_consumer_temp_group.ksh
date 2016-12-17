#!/bin/bash

PIDS=$(ps ax |  grep csv|grep consumer | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No kafka server to stop"
  exit 1
else 
  kill -s TERM $PIDS
fi

