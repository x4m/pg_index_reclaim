#!/bin/bash

# Script to run reclaim_space_execute and show output in real-time
# Usage: ./test_with_output.sh

PSQL="/home/kwolak/pgsql/17/bin/psql"
PORT=5417
DB=postgres
INDEX="idx_test_bloat_data"

echo "=========================================="
echo "Testing pg_index_reclaim extension"
echo "=========================================="
echo ""
echo "Running: SELECT * FROM reclaim_space_execute('$INDEX'::regclass, 20);"
echo ""
echo "Output (with client_min_messages = 'log'):"
echo "=========================================="
echo ""

$PSQL -p $PORT -d $DB <<EOF
SET client_min_messages = 'log';
SELECT * FROM reclaim_space_execute('$INDEX'::regclass, 20);
EOF

echo ""
echo "=========================================="
echo "Test completed. Checking server status..."
/home/kwolak/pgsql/17/bin/pg_isready -p $PORT && echo "Server is running" || echo "Server crashed!"

