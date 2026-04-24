-- Data Warehouse Schema (Star Schema)
-- SQLite

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT,
    segment TEXT,
    country TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    region TEXT,
    loaded_at TEXT
);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    sub_category TEXT,
    loaded_at TEXT
);

-- Dimension: Dates
CREATE TABLE IF NOT EXISTS dim_dates (
    date_key TEXT PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    day_of_week INTEGER,
    week_of_year INTEGER,
    is_weekend INTEGER,
    loaded_at TEXT
);

-- Fact: Sales
CREATE TABLE IF NOT EXISTS fact_sales (
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
);

-- KPIs for dashboard
CREATE TABLE IF NOT EXISTS kpi_date (
    year_month TEXT PRIMARY KEY,
    total_sales REAL,
    total_profit REAL,
    total_quantity INTEGER,
    order_count INTEGER,
    loaded_at TEXT
);

CREATE TABLE IF NOT EXISTS kpi_product (
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
);

CREATE TABLE IF NOT EXISTS kpi_customer (
    customer_id TEXT,
    customer_name TEXT,
    segment TEXT,
    total_sales REAL,
    total_profit REAL,
    order_count INTEGER,
    rank INTEGER,
    loaded_at TEXT,
    PRIMARY KEY (customer_id, rank)
);

CREATE TABLE IF NOT EXISTS kpi_category (
    category TEXT PRIMARY KEY,
    total_sales REAL,
    total_profit REAL,
    total_quantity INTEGER,
    loaded_at TEXT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(order_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_customers_region ON dim_customers(region);
CREATE INDEX IF NOT EXISTS idx_dim_products_category ON dim_products(category);