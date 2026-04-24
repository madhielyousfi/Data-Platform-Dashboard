#!/usr/bin/env python3
"""
Ingestion script - loads raw CSV data into staging/bronze zone.
Uses Python stdlib only.
"""
import csv
import json
import sys
from datetime import datetime
from pathlib import Path


BASE_PATH = Path("data-platform")


def discover_csv_files():
    raw_path = BASE_PATH / "data" / "raw"
    files = []
    if raw_path.exists():
        for f in raw_path.glob("*.csv"):
            files.append((f.stem, f))
    return files


def csv_to_json(name, filepath):
    json_path = BASE_PATH / "data" / "staging" / f"{name}.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, "r", newline="") as f_in:
        reader = csv.DictReader(f_in)
        rows = list(reader)
    
    with open(json_path, "w") as f_out:
        json.dump(rows, f_out, indent=2, default=str)
    
    print(f"[INGEST] Saved bronze: {json_path} ({len(rows)} rows)")
    return json_path


def main():
    print(f"[INGEST] Starting ingestion at {datetime.now()}")
    
    files = discover_csv_files()
    if not files:
        print("[INGEST] No CSV files found in data/raw/")
        sys.exit(1)
    
    for name, filepath in files:
        print(f"[INGEST] Loading {name} from {filepath}")
        csv_to_json(name, filepath)
    
    print("[INGEST] Ingestion complete")


if __name__ == "__main__":
    main()