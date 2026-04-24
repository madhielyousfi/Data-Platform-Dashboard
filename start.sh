#!/bin/bash
# Data Platform - Start ETL + Dashboard
# Usage: ./start.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "Data Platform - Starting..."
echo "=========================================="

# Kill existing processes
fuser -k 8888/tcp 2>/dev/null || true
pkill -f "dashboard/server.py" 2>/dev/null || true
sleep 1

# Step 1: Run ETL Pipeline
echo ""
echo "=========================================="
echo "[STEP 1/2] Running ETL Pipeline..."
echo "=========================================="
./run_pipeline.sh

# Step 2: Start Dashboard
echo ""
echo "=========================================="
echo "[STEP 2/2] Starting Dashboard..."
echo "=========================================="
nohup python3 dashboard/server.py > /tmp/dashboard.log 2>&1 &
sleep 2

# Show results
echo ""
echo "=========================================="
echo "✓ Data Platform Ready!"
echo "=========================================="
echo ""
echo "┌──────────────────────────────────────┐"
echo "│  URLs                               │"
echo "├──────────────────────────────────────┤"
echo "│  Dashboard:  http://localhost:8888   │"
echo "│  Database:   warehouse/datawarehouse │"
echo "│  Query cmd: sqlite3 warehouse/datawarehouse.db"
echo "└──────────────────────────────────────┘"
echo ""
echo "Quick commands:"
echo "  ./start.sh              # Run both"
echo "  ./run_pipeline.sh      # ETL only"
echo "  python3 dashboard/server.py  # Dashboard only"
echo ""
echo "For full tutorial: cat README.md"