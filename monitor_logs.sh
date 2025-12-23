#!/bin/bash
# Monitor PostgreSQL logs for pg_index_reclaim messages

LOG_DIR="/home/kwolak/pgsql/17/data/log"
LATEST_LOG=$(find "$LOG_DIR" -name "*.log" -type f 2>/dev/null | xargs ls -t | head -1)

if [ -z "$LATEST_LOG" ]; then
    echo "No log file found in $LOG_DIR"
    exit 1
fi

echo "Monitoring log file: $LATEST_LOG"
echo "Press Ctrl+C to stop"
echo ""

tail -f "$LATEST_LOG" | grep --line-buffered -E "pg_index_reclaim|ERROR|FATAL|PANIC"

