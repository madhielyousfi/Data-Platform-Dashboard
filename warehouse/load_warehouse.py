#!/usr/bin/env python3
"""
Warehouse load script - loads gold data into SQLite.
Uses Python stdlib only (sqlite3 built-in).
"""
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

BASE_PATH = Path("/home/dados/Documents/Data Platform")


def get_db_connection():
    db_path = BASE_PATH / "warehouse" / "datawarehouse.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn):
    cursor = conn.cursor()
    
    tables = [
        """CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT,
            segment TEXT,
            country TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            region TEXT,
            loaded_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS dim_products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            sub_category TEXT,
            loaded_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS dim_dates (
            date_key TEXT PRIMARY KEY,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            quarter INTEGER,
            day_of_week INTEGER,
            week_of_year INTEGER,
            is_weekend INTEGER,
            loaded_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS fact_sales (
            sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT,
            order_date TEXT,
            ship_date TEXT,
            ship_mode TEXT,
            customer_id TEXT,
            product_id TEXT,
            quantity INTEGER,
            sales REAL,
            discount REAL,
            profit REAL,
            loaded_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS kpi_date (
            year_month TEXT PRIMARY KEY,
            total_sales REAL,
            total_profit REAL,
            total_quantity INTEGER,
            order_count INTEGER,
            loaded_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS kpi_product (
            product_id TEXT,
            product_name TEXT,
            category TEXT,
            sub_category TEXT,
            total_sales REAL,
            total_profit REAL,
            total_quantity INTEGER,
            rank INTEGER,
            loaded_at TEXT,
            PRIMARY KEY (product_id, rank)
        )""",
        """CREATE TABLE IF NOT EXISTS kpi_customer (
            customer_id TEXT,
            customer_name TEXT,
            segment TEXT,
            total_sales REAL,
            total_profit REAL,
            order_count INTEGER,
            rank INTEGER,
            loaded_at TEXT,
            PRIMARY KEY (customer_id, rank)
        )""",
        """CREATE TABLE IF NOT EXISTS kpi_category (
            category TEXT PRIMARY KEY,
            total_sales REAL,
            total_profit REAL,
            total_quantity INTEGER,
            loaded_at TEXT
        )"""
    ]
    
    for stmt in tables:
        cursor.execute(stmt)
    
    conn.commit()
    print("[WAREHOUSE] Schema initialized")


def load_customers(conn):
    silver_path = BASE_PATH / "data-platform" / "data" / "silver" / "superstore.json"
    with open(silver_path, "r") as f:
        rows = json.load(f)
    
    cursor = conn.cursor()
    customers = {}
    for row in rows:
        cid = row.get("Customer_ID", "")
        if cid and cid not in customers:
            customers[cid] = {
                "customer_id": cid,
                "customer_name": row.get("Customer_Name", ""),
                "segment": row.get("Segment", ""),
                "country": row.get("Country", ""),
                "city": row.get("City", ""),
                "state": row.get("State", ""),
                "postal_code": row.get("Postal_Code", ""),
                "region": row.get("Region", ""),
                "loaded_at": datetime.now().isoformat()
            }
    
    cursor.executemany(
        """INSERT OR REPLACE INTO dim_customers 
           (customer_id, customer_name, segment, country, city, state, postal_code, region, loaded_at)
           VALUES (:customer_id, :customer_name, :segment, :country, :city, :state, :postal_code, :region, :loaded_at)""",
        list(customers.values())
    )
    conn.commit()
    print(f"[WAREHOUSE] Loaded {len(customers)} customers")


def load_products(conn):
    silver_path = BASE_PATH / "data-platform" / "data" / "silver" / "superstore.json"
    with open(silver_path, "r") as f:
        rows = json.load(f)
    
    cursor = conn.cursor()
    products = {}
    for row in rows:
        pid = row.get("Product_ID", "")
        if pid and pid not in products:
            products[pid] = {
                "product_id": pid,
                "product_name": row.get("Product_Name", ""),
                "category": row.get("Category", ""),
                "sub_category": row.get("Sub_Category", ""),
                "loaded_at": datetime.now().isoformat()
            }
    
    cursor.executemany(
        """INSERT OR REPLACE INTO dim_products 
           (product_id, product_name, category, sub_category, loaded_at)
           VALUES (:product_id, :product_name, :category, :sub_category, :loaded_at)""",
        list(products.values())
    )
    conn.commit()
    print(f"[WAREHOUSE] Loaded {len(products)} products")


def load_fact_sales(conn):
    silver_path = BASE_PATH / "data-platform" / "data" / "silver" / "superstore.json"
    with open(silver_path, "r") as f:
        rows = json.load(f)
    
    cursor = conn.cursor()
    facts = []
    for row in rows:
        facts.append({
            "order_id": row.get("Order_ID", ""),
            "order_date": row.get("Order_Date", ""),
            "ship_date": row.get("Ship_Date", ""),
            "ship_mode": row.get("Ship_Mode", ""),
            "customer_id": row.get("Customer_ID", ""),
            "product_id": row.get("Product_ID", ""),
            "quantity": int(row.get("Quantity", 0)),
            "sales": float(row.get("Sales", 0)),
            "discount": float(row.get("Discount", 0)),
            "profit": float(row.get("Profit", 0)),
            "loaded_at": datetime.now().isoformat()
        })
    
    cursor.executemany(
        """INSERT INTO fact_sales 
           (order_id, order_date, ship_date, ship_mode, customer_id, product_id, quantity, sales, discount, profit, loaded_at)
           VALUES (:order_id, :order_date, :ship_date, :ship_mode, :customer_id, :product_id, :quantity, :sales, :discount, :profit, :loaded_at)""",
        facts
    )
    conn.commit()
    print(f"[WAREHOUSE] Loaded {len(facts)} sales records")


def load_kpis(conn):
    gold_path = BASE_PATH / "data-platform" / "data" / "gold"
    
    cursor = conn.cursor()
    
    for table_name in ["kpi_date", "kpi_product", "kpi_customer", "kpi_category"]:
        fpath = gold_path / f"{table_name}.json"
        if fpath.exists():
            with open(fpath, "r") as f:
                data = json.load(f)
            
            for row in data:
                row["loaded_at"] = datetime.now().isoformat()
            
            if table_name == "kpi_date":
                cursor.executemany(
                    """INSERT OR REPLACE INTO kpi_date (year_month, total_sales, total_profit, total_quantity, order_count, loaded_at)
                       VALUES (:year_month, :total_sales, :total_profit, :total_quantity, :order_count, :loaded_at)""",
                    data
                )
            elif table_name == "kpi_product":
                cursor.executemany(
                    """INSERT OR REPLACE INTO kpi_product (product_id, product_name, category, sub_category, total_sales, total_profit, total_quantity, rank, loaded_at)
                       VALUES (:product_id, :product_name, :category, :sub_category, :total_sales, :total_profit, :total_quantity, :rank, :loaded_at)""",
                    data
                )
            elif table_name == "kpi_customer":
                cursor.executemany(
                    """INSERT OR REPLACE INTO kpi_customer (customer_id, customer_name, segment, total_sales, total_profit, order_count, rank, loaded_at)
                       VALUES (:customer_id, :customer_name, :segment, :total_sales, :total_profit, :order_count, :rank, :loaded_at)""",
                    data
                )
            elif table_name == "kpi_category":
                cursor.executemany(
                    """INSERT OR REPLACE INTO kpi_category (category, total_sales, total_profit, total_quantity, loaded_at)
                       VALUES (:category, :total_sales, :total_profit, :total_quantity, :loaded_at)""",
                    data
                )
            
            conn.commit()
            print(f"[WAREHOUSE] Loaded {table_name}")


def main():
    print(f"[WAREHOUSE] Starting load at {datetime.now()}")
    
    try:
        conn = get_db_connection()
        print("[WAREHOUSE] Connected to SQLite")
    except Exception as e:
        print(f"[WAREHOUSE] Failed to connect: {e}")
        sys.exit(1)
    
    try:
        init_schema(conn)
        load_customers(conn)
        load_products(conn)
        load_fact_sales(conn)
        load_kpis(conn)
        conn.close()
        print("[WAREHOUSE] Load complete")
    except Exception as e:
        import traceback
        print(f"[WAREHOUSE] Error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()