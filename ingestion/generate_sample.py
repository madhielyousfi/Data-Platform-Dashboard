#!/usr/bin/env python3
"""
Generate sample Superstore-like data for testing.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_PATH = "data-platform/data/raw/superstore.csv"

CATEGORIES = ["Furniture", "Office Supplies", "Technology"]
SUB_CATEGORIES = {
    "Furniture": ["Chairs", "Tables", "Bookcases", "Furnishings"],
    "Office Supplies": ["Labels", "Paper", "Binders", "Storage"],
    "Technology": ["Phones", "Computers", "Copiers", "Accessories"]
}
SEGMENTS = ["Consumer", "Corporate", "Home Office"]
REGIONS = ["East", "West", "Central", "South"]
STATES = {
    "East": ["New York", "Pennsylvania", "Massachusetts"],
    "West": ["California", "Oregon", "Washington"],
    "Central": ["Illinois", "Texas", "Ohio"],
    "South": ["Florida", "Georgia", "North Carolina"]
}
SHIP_MODES = ["Standard Class", "Second Class", "First Class", "Same Day"]

CUSTOMER_NAMES = ["John Smith", "Jane Doe", "Bob Johnson", "Alice Brown", "Charlie Wilson",
                "Diana Miller", "Edward Davis", "Fiona Garcia", "George Martinez", "Hannah Lee"]
PRODUCT_NAMES = {
    "Chairs": ["Ergonomic Chair", "Executive Chair", "Conference Chair"],
    "Tables": ["Dining Table", "Coffee Table", "Conference Table"],
    "Bookcases": ["Standard Bookcase", "Corner Bookcase"],
    "Furnishes": [" Lamp", "Rug", "Mirror"],
    "Labels": ["Address Labels", "Shipping Labels"],
    "Paper": ["Copy Paper", "Notebook", "Sticky Notes"],
    "Binders": ["3-Ring Binder", "5-Ring Binder"],
    "Storage": ["File Box", "Storage Bin"],
    "Phones": ["Office Phone", "Mobile Phone"],
    "Computers": ["Laptop", "Desktop", "Tablet"],
    "Copiers": ["Copier Machine"],
    "Accessories": ["Mouse", "Keyboard", "Webcam"]
}


def random_date(start, end):
    delta = (end - start).days
    random_days = random.randint(0, delta)
    return (start + timedelta(days=random_days)).strftime("%m/%d/%Y")


def generate_record(order_id):
    segment = random.choice(SEGMENTS)
    region = random.choice(REGIONS)
    state = random.choice(STATES[region])
    category = random.choice(CATEGORIES)
    sub_category = random.choice(SUB_CATEGORIES[category])
    product_name = random.choice(PRODUCT_NAMES.get(sub_category, ["Product"]))
    
    order_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
    ship_date = order_date + timedelta(days=random.randint(1, 7))
    
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(10, 500), 2)
    discount = round(random.uniform(0, 0.3), 2)
    sales = round(quantity * unit_price * (1 - discount), 2)
    profit = round(sales * random.uniform(0.1, 0.4), 2)
    
    return {
        "Row_ID": order_id,
        "Order_ID": f"OR-{order_id:06d}",
        "Order_Date": order_date.strftime("%m/%d/%Y"),
        "Ship_Date": ship_date.strftime("%m/%d/%Y"),
        "Ship_Mode": random.choice(SHIP_MODES),
        "Customer_ID": f"CUST-{random.randint(1, 500):04d}",
        "Customer_Name": random.choice(CUSTOMER_NAMES),
        "Segment": segment,
        "Country": "United States",
        "City": random.choice(["New York", "Los Angeles", "Chicago", "Houston"]),
        "State": state,
        "Postal_Code": f"{random.randint(10000, 99999)}",
        "Region": region,
        "Product_ID": f"PROD-{random.randint(1, 100):04d}",
        "Category": category,
        "Sub_Category": sub_category,
        "Product_Name": product_name,
        "Sales": sales,
        "Quantity": quantity,
        "Discount": discount,
        "Profit": profit
    }


def main():
    Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    
    fieldnames = ["Row_ID", "Order_ID", "Order_Date", "Ship_Date", "Ship_Mode", "Customer_ID",
               "Customer_Name", "Segment", "Country", "City", "State", "Postal_Code",
               "Region", "Product_ID", "Category", "Sub_Category", "Product_Name",
               "Sales", "Quantity", "Discount", "Profit"]
    
    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for i in range(1, 1001):
            writer.writerow(generate_record(i))
    
    print(f"[GENERATE] Created {OUTPUT_PATH} with 1000 records")


if __name__ == "__main__":
    main()