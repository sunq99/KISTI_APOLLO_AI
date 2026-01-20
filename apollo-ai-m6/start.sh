#!/bin/bash

export APP_ENV=${1:-"development"}
export UNICORN_PORT=${2:-"8002"}
UNICORN_PID_FILE="unicorn.${APP_ENV}.pid"
UNICORN_LOG_FILE="unicorn.${APP_ENV}.log"

echo "Starting Unicorn in environment: $APP_ENV"
echo "Binding to port: $UNICORN_PORT"
echo "Using PID file: $UNICORN_PID_FILE"

# Uvicorn 실행 (base poetry)
BUILD_ID=dontKillMe nohup poetry run gunicorn main:app \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:$UNICORN_PORT \
  --pid $UNICORN_PID_FILE \
  --workers 4 > $UNICORN_LOG_FILE 2>&1 &