#!/bin/bash
# Start the Dashboard Web Server
cd "/home/twarga/data_platform/Data-Platform-Dashboard-dados"

echo "Starting Data Platform Dashboard..."
echo "URL: http://localhost:8888"

# Kill any existing process
pkill -f "dashboard/server.py" 2>/dev/null || true
fuser -k 8888/tcp 2>/dev/null || true
sleep 1

# Start the server in background
nohup python3 dashboard/server.py > /tmp/dashboard.log 2>&1 &
SERVERPID=$!

sleep 2

# Check if running
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8888/ | grep -q "200"; then
    echo "Dashboard running! PID: $SERVERPID"
    echo ""
    echo "Open browser: http://localhost:8888"
else
    echo "Checking for error..."
    cat /tmp/dashboard.log
fi