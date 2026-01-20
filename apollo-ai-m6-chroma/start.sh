#!/bin/bash

export APP_ENV=${1:-"development"}
export GUNICORN_PORT=${2:-"8004"}
GUNICORN_PID_FILE="gunicorn.${APP_ENV}.pid"
GUNICORN_LOG_FILE="gunicorn.${APP_ENV}.log"

echo "Starting Gunicorn in environment: $APP_ENV"
echo "Binding to port: $GUNICORN_PORT"
echo "Using PID file: $GUNICORN_PID_FILE"

# Gunicorn 실행 (base poetry)
BUILD_ID=dontKillMe nohup poetry run gunicorn \
  --bind 0.0.0.0:$GUNICORN_PORT \
  --pid $GUNICORN_PID_FILE \
  --workers 4 \
  main:app > $GUNICORN_LOG_FILE 2>&1 &