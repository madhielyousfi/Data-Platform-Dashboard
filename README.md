# Data Platform - Complete Tutorial

This tutorial walks you through building a complete **ETL Data Platform** from scratch.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Project Structure](#2-project-structure)
3. [Quick Start](#3-quick-start)
4. [Step-by-Step Guide](#4-step-by-step-guide)
5. [Dashboard Usage](#5-dashboard-usage)
6. [API Reference](#6-api-reference)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA PLATFORM                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐│
│  │  CSV   │───▶│Bronze  │───▶│Silver  │───▶│ Gold   ││
│  │ Source │    │ (raw)  │    │(clean) │    │ (KPIs) ││
│  └─────────┘    └────────┘    └────────┘    └────────┘│
│                          │            │            │        │
│                          └────────────┴────────────┘        │
│                                       │                 │
│                                  ┌────▼────┐          │
│                                  │SQLite   │          │
│                                  │Warehouse│          │
│                                  └────────┘          │
│                                       │                 │
│                                  ┌────▼────┐          │
│                                  │Dashboard│          │
│                                  │  (Web) │          │
│                                  └────────┘          │
└─────────────────────────────────────────────────────────────┘
```

### Layers

| Layer | Technology | Description |
|-------|-----------|------------|
| **Source** | CSV | Raw data files |
| **Bronze** | JSON | Raw data (no changes) |
| **Silver** | JSON | Cleaned, validated |
| **Gold** | JSON | Business KPIs |
| **Warehouse** | SQLite | Star schema DB |
| **Serving** | Web Dashboard | UI for data |

---

## 2. Project Structure

```
Data Platform/
├── start.sh                      # Main script: runs pipeline + dashboard
├── run_pipeline.sh               # ETL pipeline only
├── plan.md                      # Architecture plan
│
├── data-platform/               # Data storage
│   ├── data/
│   │   ├── raw/                # CSV files (your source data)
│   │   ├── staging/            # Bronze zone (raw JSON)
│   │   ├── silver/            # Silver zone (cleaned)
│   │   └── gold/              # Gold zone (KPIs)
│   │
│   └── warehouse/              # Empty (generated)
│
├── ingestion/                  # Data ingestion scripts
│   ├── generate_sample.py     # Generate test data
│   └── load_raw.py           # CSV → Bronze
│
├── processing/                # ETL processing scripts
│   ├── clean.py              # Bronze → Silver
│   └── aggregate.py         # Silver → Gold KPIs
│
├── warehouse/                # Database layer
│   ├── schema.sql            # SQLite schema
│   ├── load_warehouse.py   # Load to SQLite
│   └── datawarehouse.db    # Generated database
│
├── dashboard/               # Web dashboard
│   ├── server.py           # Web server
│   └── start.sh          # Start dashboard
│
└── config/               # Configuration
    └── sources.yaml       # Data source config
```

---

## 3. Quick Start

### Option 1: Run Everything (Pipeline + Dashboard)

```bash
cd "/home/dados/Documents/Data Platform"
./start.sh
```

**Output:**
- Pipeline runs ETL
- Dashboard starts at http://localhost:8888

### Option 2: Run Pipeline Only

```bash
cd "/home/dados/Documents/Data Platform"
./run_pipeline.sh
```

### Option 3: Start Dashboard Only

```bash
cd "/home/dados/Documents/Data Platform"
python3 dashboard/server.py
```

Then open: http://localhost:8888

---

## 4. Step-by-Step Guide

### Step 1: Generate Sample Data

**File:** `ingestion/generate_sample.py`

Creates 1000 sample sales records.

```bash
python3 ingestion/generate_sample.py
```

**Output:** `data-platform/data/raw/superstore.csv`

**Sample record:**
```
Order_ID, Order_Date, Ship_Date, Ship_Mode, Customer_ID, Customer_Name, Segment, 
City, State, Region, Product_ID, Category, Sub_Category, Product_Name, 
Sales, Quantity, Discount, Profit
```

---

### Step 2: Ingestion (CSV → Bronze)

**File:** `ingestion/load_raw.py`

Reads CSV file and converts to JSON (Bronze zone).

```bash
python3 ingestion/load_raw.py
```

**Output:**
- Input: `data-platform/data/raw/superstore.csv`
- Output: `data-platform/data/staging/superstore.json`

**Format:**
```json
[
  {
    "Row_ID": "1",
    "Order_ID": "OR-000001",
    "Order_Date": "01/01/2023",
    ...
  }
]
```

---

### Step 3: Clean (Bronze → Silver)

**File:** `processing/clean.py`

Cleans and validates data:
- Remove nulls
- Convert data types
- Add audit columns

```bash
python3 processing/clean.py
```

**Output:** `data-platform/data/silver/superstore.json`

**Transformations:**
- Removes rows with missing Order_ID, Order_Date, Customer_ID
- Converts Sales, Quantity, Discount, Profit to numbers
- Adds `_loaded_at` timestamp

---

### Step 4: Aggregate (Silver → Gold)

**File:** `processing/aggregate.py`

Creates business KPIs.

```bash
python3 processing/aggregate.py
```

**Output:** Multiple KPI files in `data-platform/data/gold/`

| File | Description |
|------|-----------|
| `kpi_date.json` | Monthly sales trends |
| `kpi_product.json` | Top 10 products |
| `kpi_customer.json` | Top 10 customers |
| `kpi_category.json` | Sales by category |

**Sample KPI:**
```json
// kpi_category.json
[
  {
    "category": "Furniture",
    "total_sales": 405159.94,
    "total_profit": 96246.41,
    "total_quantity": 1898
  }
]
```

---

### Step 5: Load to Warehouse

**File:** `warehouse/load_warehouse.py`

Loads data to SQLite database with star schema.

```bash
python3 warehouse/load_warehouse.py
```

**Database:** `warehouse/datawarehouse.db`

**Schema:**

```
┌─────────────┐     ┌─────────────┐
│dim_customers │◄────│ fact_sales │
│dim_products │◄────└───────────┘
└─────────────┘     └─────────────┘

┌─────────────┐
│kpi_category│
│kpi_date   │
│kpi_product│
│kpi_customer│
└─────────────┘
```

---

## 5. Dashboard Usage

### Access

**URL:** http://localhost:8888

### Pages

| Page | URL | Description |
|------|-----|-----------|
| Dashboard | / | Stats, charts, KPIs |
| Data | /data | Browse tables |
| Run | /run | Run pipeline, export CSV |

### Dashboard Features

1. **Stats Cards** - Customers, Products, Sales, Revenue
2. **Top Customers** - By sales
3. **Top Products** - By category
4. **Category Breakdown** - Sales by category
5. **Monthly Trend** - Sales over time

### Run Pipeline Tab

Click **"Run Pipeline"** to:
- Generate new data
- Run full ETL
- Refresh dashboard stats

Click **"Export CSV"** to:
- Download sales data as CSV file

---

## 6. API Reference

### Web Endpoints

| Method | URL | Description |
|--------|-----|-----------|
| GET | / | Dashboard homepage |
| GET | /data | Data explorer |
| GET | /run | Pipeline controls |
| GET | /api/run | Run ETL pipeline |
| GET | /api/export | Download CSV |

### Database Tables

```sql
-- Dimension tables
SELECT * FROM dim_customers;
SELECT * FROM dim_products;

-- Fact table
SELECT * FROM fact_sales;

-- KPIs
SELECT * FROM kpi_category;
SELECT * FROM kpi_date;
SELECT * FROM kpi_product;
SELECT * FROM kpi_customer;
```

### Query Examples

```bash
# Connect to database
sqlite3 warehouse/datawarehouse.db

# Show all tables
.tables

# Show stats
SELECT 'Customers:' || COUNT(*) FROM dim_customers;
SELECT 'Products:' || COUNT(*) FROM dim_products;
SELECT 'Sales:' || COUNT(*) FROM fact_sales;

# Top categories
SELECT * FROM kpi_category ORDER BY total_sales DESC;

# Monthly trend
SELECT * FROM kpi_date ORDER BY year_month;
```

---

## Troubleshooting

### Port already in use

```bash
fuser -k 8888/tcp
```

### Start fresh

```bash
# Kill dashboard
pkill -f dashboard/server.py

# Run pipeline fresh
./run_pipeline.sh

# Start dashboard
python3 dashboard/server.py
```

---

## Files Created in This Project

| File | Purpose |
|------|---------|
| `start.sh` | Main entry point |
| `run_pipeline.sh` | ETL pipeline |
| `run_pipeline.py` | Python ETL wrapper |
| `ingestion/generate_sample.py` | Mock data generator |
| `ingestion/load_raw.py` | Load CSV to JSON |
| `processing/clean.py` | Clean data |
| `processing/aggregate.py` | Create KPIs |
| `warehouse/schema.sql` | SQLite schema |
| `warehouse/load_warehouse.py` | Load to DB |
| `dashboard/server.py` | Web UI |
| `dashboard/start.sh` | Start dashboard |

---

## Next Steps (Advanced)

- [ ] Add Airflow orchestration
- [ ] Add data quality checks
- [ ] Add streaming (Kafka)
- [ ] Add API layer (FastAPI)
- [ ] Add authentication
- [ ] Add scheduled jobs (cron)
- [ ] Add more data sources
- [ ] Deploy to cloud

---

**End of Tutorial**

For questions, check the code comments or ask!