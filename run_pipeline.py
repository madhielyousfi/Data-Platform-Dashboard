#!/usr/bin/env python3
"""
Run the full ETL pipeline.
Usage: python3 run_pipeline.py
"""
import subprocess
import sys
from pathlib import Path
from datetime import datetime


def run_step(name, cmd):
    print(f"\n{'='*50}")
    print(f"[PIPELINE] Step: {name}")
    print(f"{'='*50}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"[PIPELINE] ERROR in {name}")
        sys.exit(result.returncode)
    print(f"[PIPELINE] {name} complete")


def main():
    base = Path("data-platform")
    scripts = base / "scripts"
    scripts.mkdir(exist_ok=True)
    
    workdir = "/home/dados/Documents/Data Platform"
    
    print(f"[PIPELINE] Starting ETL pipeline at {datetime.now()}")
    print(f"[PIPELINE] Working directory: {workdir}")
    
    # Step 1: Generate sample data
    run_step("Generate Sample Data", 
            f'cd "{workdir}" && python3 ingestion/generate_sample.py')
    
    # Step 2: Ingestion
    run_step("Ingestion (load_raw)", 
            f'cd "{workdir}" && python3 ingestion/load_raw.py')
    
    # Step 3: Processing - Bronze to Silver
    run_step("Processing (clean)", 
            f'cd "{workdir}" && python3 processing/clean.py')
    
    # Step 4: Processing - Silver to Gold
    run_step("Processing (aggregate)", 
            f'cd "{workdir}" && python3 processing/aggregate.py')
    
    # Step 5: Load to Warehouse
    run_step("Load to Warehouse", 
            f'cd "{workdir}" && python3 warehouse/load_warehouse.py')
    
    print(f"\n{'='*50}")
    print(f"[PIPELINE] Pipeline complete at {datetime.now()}")
    print(f"{'='*50}")
    
    # Show summary
    db_path = Path(workdir) / "data-platform" / "warehouse" / "datawarehouse.db"
    if db_path.exists():
        print(f"\n[INFO] SQLite database: {db_path}")
        print("[INFO] Query with: sqlite3 data-platform/warehouse/datawarehouse.db")


if __name__ == "__main__":
    main()