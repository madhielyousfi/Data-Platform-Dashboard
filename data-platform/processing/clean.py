#!/usr/bin/env python3
"""
Processing script - transforms bronze to silver (clean + normalize).
"""
import json
import sys
from datetime import datetime
from pathlib import Path

BASE_PATH = Path("/home/dados/Documents/Data Platform/data-platform")


def load_bronze(name):
    bronze_path = BASE_PATH / "lake" / "bronze" / f"{name}.json"
    with open(bronze_path, "r") as f:
        return json.load(f)


def clean_data(rows):
    print(f"[CLEAN] Cleaning {len(rows)} rows")
    
    cleaned = []
    for row in rows:
        if not row.get("Order_ID") or not row.get("Order_Date") or not row.get("Customer_ID"):
            continue
        
        row["_loaded_at"] = datetime.now().isoformat()
        
        for key in ["Sales", "Quantity", "Discount", "Profit"]:
            if key in row and row[key]:
                try:
                    row[key] = float(row[key])
                except (ValueError, TypeError):
                    row[key] = 0.0
        
        cleaned.append(row)
    
    print(f"[CLEAN] Cleaned {len(cleaned)} rows")
    return cleaned


def save_silver(rows, name):
    silver_path = BASE_PATH / "lake" / "silver" / f"{name}.json"
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(silver_path, "w") as f:
        json.dump(rows, f, indent=2)
    
    print(f"[CLEAN] Saved silver: {silver_path}")
    return silver_path


def main():
    print(f"[CLEAN] Starting at {datetime.now()}")
    
    sources = ["superstore"]
    for name in sources:
        try:
            rows = load_bronze(name)
            cleaned = clean_data(rows)
            save_silver(cleaned, name)
        except Exception as e:
            print(f"[CLEAN] Error: {e}")
            sys.exit(1)
    
    print("[CLEAN] Bronze→Silver complete")


if __name__ == "__main__":
    main()