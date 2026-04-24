# 📊 Data Platform Dashboard

A complete ETL data platform built with **Python stdlib only** - no external dependencies!

![Python](https://img.shields.io/badge/Python-3.14-blue)
![SQLite](https://img.shields.io/badge/SQLite-3-brightgreen)
![Dashboard](https://img.shields.io/badge/Dashboard-Web-orange)

---

## ⚡ Quick Start

```bash
# Run everything (pipeline + dashboard)
./start.sh
```

That's it! Then open ➜ **http://localhost:8888**

---

## 🚀 Features

| Feature | Description |
|---------|-------------|
| **ETL Pipeline** | Generate → Ingest → Clean → Aggregate → Load |
| **SQLite DB** | Star schema warehouse |
| **Web Dashboard** | Stats, charts, KPIs |
| **Export CSV** | One-click data export |
| **Zero Dependencies** | Pure Python stdlib |

---

## 📁 Project Structure

```
Data Platform/
├── start.sh                 # Main: run pipeline + dashboard
├── run_pipeline.sh         # ETL pipeline
│
├── ingestion/              # Data ingestion
│   ├── generate_sample.py # Generate test data
│   └── load_raw.py         # CSV → JSON
│
├── processing/            # ETL transforms
│   ├── clean.py           # Clean & validate
│   └── aggregate.py      # Create KPIs
│
├── warehouse/             # Database
│   ├── schema.sql         # SQLite schema
│   └── load_warehouse.py # Load to DB
│
└── dashboard/             # Web UI
    └── server.py          # Dashboard server
```

---

## 📖 Usage

### 1. Run Full Platform

```bash
./start.sh
```

**Opens:** http://localhost:8888

### 2. Run Pipeline Only

```bash
./run_pipeline.sh
```

### 3. Start Dashboard Only

```bash
python3 dashboard/server.py
```

---

## 🖥️ Dashboard Pages

| Page | URL | Description |
|------|-----|-------------|
| **Dashboard** | `/` | Stats & KPIs |
| **Data** | `/data` | Browse tables |
| **Run** | `/run` | Run pipeline, export CSV |

---

## 📊 What's Built

### Data Flow

```
CSV Source → Bronze (raw) → Silver (clean) → Gold (KPIs) → SQLite → Dashboard
```

### Database Schema (Star)

```
dim_customers ────┐
               ├──→ fact_sales
dim_products ──┘
               
kpi_category
kpi_date       
kpi_product    
kpi_customer   
```

### KPIs Generated

- 📈 Monthly sales trends
- 🏆 Top 10 products
- 👥 Top 10 customers
- 📦 Sales by category

---

## 🔧 Troubleshooting

### Port in use?

```bash
fuser -k 8888/tcp
```

### Start fresh

```bash
./start.sh
```

---

## 🤝 Contributing

1. Fork it
2. Create branch (`git checkout -b feature`)
3. Commit (`git commit -m "Add feature"`)
4. Push (`git push origin main`)

---

## 📝 License

MIT

---

**Built with** ❤️ using pure Python stdlib