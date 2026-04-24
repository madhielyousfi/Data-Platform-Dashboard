#!/bin/bash
# Data Platform - Run ETL Pipeline
# Usage: ./run_pipeline.sh

set -e

WORKDIR="/home/dados/Documents/Data Platform"
cd "$WORKDIR"

echo "========================================"
echo "[PIPELINE] Starting ETL Pipeline"
echo "========================================"
echo "Time: $(date)"
echo ""

# Step 1: Generate sample data
echo "[STEP 1] Generate Sample Data"
python3 ingestion/generate_sample.py

# Step 2: Ingestion (load raw CSV → bronze JSON)
echo ""
echo "[STEP 2] Ingestion (load_raw)"
python3 ingestion/load_raw.py

# Step 3: Processing - Bronze to Silver (clean)
echo ""
echo "[STEP 3] Processing (clean - bronze→silver)"
python3 processing/clean.py

# Step 4: Processing - Silver to Gold (aggregate KPIs)
echo ""
echo "[STEP 4] Processing (aggregate - silver→gold)"
python3 processing/aggregate.py

# Step 5: Load to Warehouse (SQLite)
echo ""
echo "[STEP 5] Load to Warehouse (SQLite)"
python3 warehouse/load_warehouse.py

echo ""
echo "========================================"
echo "[PIPELINE] Pipeline Complete!"
echo "========================================"
echo "Time: $(date)"
echo ""
echo "Database: $WORKDIR/warehouse/datawarehouse.db"
echo "Query with: sqlite3 $WORKDIR/warehouse/datawarehouse.db"
echo ""

# Show summary
echo "--- Quick Stats ---"
sqlite3 "$WORKDIR/warehouse/datawarehouse.db" "SELECT 'Customers: ' || COUNT(*) FROM dim_customers; SELECT 'Products: ' || COUNT(*) FROM dim_products; SELECT 'Sales: ' || COUNT(*) FROM fact_sales; SELECT 'Categories: ' || COUNT(*) FROM kpi_category;"