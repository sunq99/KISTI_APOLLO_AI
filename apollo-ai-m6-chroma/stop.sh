#!/bin/bash

export APP_ENV=${1:-"development"}

# Gunicorn PID 파일 경로
PID_FILE="gunicorn.${APP_ENV}.pid"

# PID 파일이 존재하는지 확인
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    echo "Stopping Gunicorn with PID: $PID"

    # 프로세스 종료
    kill "$PID"

    # 종료 후 PID 파일 삭제
    rm -f "$PID_FILE"

    echo "Gunicorn stopped."
else
    echo "Gunicorn PID file not found. Is Gunicorn running?"
fi