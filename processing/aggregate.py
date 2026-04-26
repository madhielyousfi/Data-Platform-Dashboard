#!/usr/bin/env python3
"""
Aggregation script - transforms silver to gold (business KPIs).
Uses Python stdlib only.
"""
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE_PATH = Path.cwd()


def load_silver(name):
    silver_path = BASE_PATH / "data" / "silver" / f"{name}.json"
    with open(silver_path, "r") as f:
        return json.load(f)


def parse_date(date_str):
    for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def aggregate(rows):
    print(f"[AGGREGATE] Computing KPIs for {len(rows)} rows")
    
    by_month = defaultdict(lambda: {"sales": 0, "profit": 0, "quantity": 0, "orders": set()})
    by_product = defaultdict(lambda: {"sales": 0, "profit": 0, "quantity": 0})
    by_customer = defaultdict(lambda: {"sales": 0, "profit": 0, "orders": set()})
    by_category = defaultdict(lambda: {"sales": 0, "profit": 0, "quantity": 0})
    
    for row in rows:
        dt = parse_date(row.get("Order_Date", ""))
        if dt:
            year_month = f"{dt.year}-{dt.month:02d}"
            by_month[year_month]["sales"] += float(row.get("Sales", 0))
            by_month[year_month]["profit"] += float(row.get("Profit", 0))
            by_month[year_month]["quantity"] += int(row.get("Quantity", 0))
            by_month[year_month]["orders"].add(row.get("Order_ID", ""))
        
        product_id = row.get("Product_ID", "")
        if product_id:
            by_product[product_id]["sales"] += float(row.get("Sales", 0))
            by_product[product_id]["profit"] += float(row.get("Profit", 0))
            by_product[product_id]["quantity"] += int(row.get("Quantity", 0))
            by_product[product_id]["name"] = row.get("Product_Name", "")
            by_product[product_id]["category"] = row.get("Category", "")
            by_product[product_id]["sub_category"] = row.get("Sub_Category", "")
        
        customer_id = row.get("Customer_ID", "")
        if customer_id:
            by_customer[customer_id]["sales"] += float(row.get("Sales", 0))
            by_customer[customer_id]["profit"] += float(row.get("Profit", 0))
            by_customer[customer_id]["orders"].add(row.get("Order_ID", ""))
            by_customer[customer_id]["name"] = row.get("Customer_Name", "")
            by_customer[customer_id]["segment"] = row.get("Segment", "")
        
        category = row.get("Category", "")
        if category:
            by_category[category]["sales"] += float(row.get("Sales", 0))
            by_category[category]["profit"] += float(row.get("Profit", 0))
            by_category[category]["quantity"] += int(row.get("Quantity", 0))
    
    kpi_date = []
    for ym, data in sorted(by_month.items()):
        kpi_date.append({
            "year_month": ym,
            "total_sales": round(data["sales"], 2),
            "total_profit": round(data["profit"], 2),
            "total_quantity": data["quantity"],
            "order_count": len(data["orders"])
        })
    
    products = [(pid, data) for pid, data in by_product.items()]
    products.sort(key=lambda x: x[1]["sales"], reverse=True)
    kpi_product = []
    for rank, (pid, data) in enumerate(products[:10], 1):
        kpi_product.append({
            "product_id": pid,
            "product_name": data.get("name", ""),
            "category": data.get("category", ""),
            "sub_category": data.get("sub_category", ""),
            "total_sales": round(data["sales"], 2),
            "total_profit": round(data["profit"], 2),
            "total_quantity": data["quantity"],
            "rank": rank
        })
    
    customers = [(cid, data) for cid, data in by_customer.items()]
    customers.sort(key=lambda x: x[1]["sales"], reverse=True)
    kpi_customer = []
    for rank, (cid, data) in enumerate(customers[:10], 1):
        kpi_customer.append({
            "customer_id": cid,
            "customer_name": data.get("name", ""),
            "segment": data.get("segment", ""),
            "total_sales": round(data["sales"], 2),
            "total_profit": round(data["profit"], 2),
            "order_count": len(data["orders"]),
            "rank": rank
        })
    
    kpi_category = []
    for cat, data in by_category.items():
        kpi_category.append({
            "category": cat,
            "total_sales": round(data["sales"], 2),
            "total_profit": round(data["profit"], 2),
            "total_quantity": data["quantity"]
        })
    kpi_category.sort(key=lambda x: x["total_sales"], reverse=True)
    
    print(f"[AGGREGATE] Generated KPIs")
    return {
        "kpi_date": kpi_date,
        "kpi_product": kpi_product,
        "kpi_customer": kpi_customer,
        "kpi_category": kpi_category
    }


def save_gold(kpis):
    gold_path = BASE_PATH / "data" / "gold"
    gold_path.mkdir(parents=True, exist_ok=True)
    
    for name, data in kpis.items():
        out_path = gold_path / f"{name}.json"
        with open(out_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"[AGGREGATE] Saved gold: {out_path}")


def main():
    print(f"[AGGREGATE] Starting silver→gold at {datetime.now()}")
    
    sources = ["superstore"]
    for name in sources:
        try:
            rows = load_silver(name)
            kpis = aggregate(rows)
            save_gold(kpis)
        except Exception as e:
            print(f"[AGGREGATE] Error aggregating {name}: {e}")
            sys.exit(1)
    
    print("[AGGREGATE] Silver→Gold complete")


if __name__ == "__main__":
    main()