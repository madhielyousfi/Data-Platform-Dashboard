# Data Platform - Production-grade ETL Platform

A complete ETL data platform with **Python** - built with production best practices.

![Python](https://img.shields.io/badge/Python-3.14-blue)
![PostgreSQL](https://img.shields.io/badge-PostgreSQL-15-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-00a漆00)
![Docker](https://img.shields.io/badge/Docker-ready-blue)

---

## ⚡ Quick Start

```bash
# Run everything (pipeline + dashboard)
cd data-platform
../start.sh
```

Then open ➜ **http://localhost:8888**

---

## 📁 Project Structure

```
data-platform/
├── ingestion/          # Data ingestion layer
│   ├── api_ingest.py   # Fetch from APIs
│   ├── csv_ingest.py   # Load CSV files
│   └── __init__.py
│
├── lake/               # Data Lake (core)
│   ├── raw/           # Raw data
│   ├── bronze/        # Structured data
│   ├── silver/        # Cleaned data
│   └── gold/         # Business-ready KPIs
│
├── processing/        # Transformations
│   ├── clean.py
│   ├── transform.py
│   ├── aggregate.py
│   └── __init__.py
│
├── warehouse/         # Data Warehouse
│   ├── schema.sql      # PostgreSQL schema
│   ├── load_postgres.py
│   └── models/
│
├── dags/            # Orchestration
│   └── pipeline_dag.py
│
├── api/             # Serving layer
│   └── app.py      # FastAPI
│
├── dashboard/       # UI
│   └── server.py
│
├── config/          # Configuration
│   └── config.yaml
│
├── utils/          # Shared utilities
│   ├── logger.py
│   ├── validator.py
│   ├── helpers.py
│   └── __init__.py
│
├── tests/           # Testing
│   └── test_pipeline.py
│
├── docker/         # Infrastructure
│   ├── docker-compose.yml
│   ├── Dockerfile.api
│   └── Dockerfile.dashboard
│
└── scripts/       # Execution
    └── run_pipeline.sh
```

---

## 🚀 Features

| Feature | Description |
|---------|-------------|
| **Data Lake** | Raw → Bronze → Silver → Gold zones |
| **ETL Pipeline** | Generate → Ingest → Clean → Aggregate → Load |
| **PostgreSQL** | Star schema warehouse |
| **FastAPI** | REST API endpoints |
| **Docker** | Containerized services |
| **Airflow** | DAG orchestration |
| **Logging** | Pipeline logging |
| **Validation** | Data quality checks |
| **Tests** | Unit tests |

---

## 📖 Usage

### 1. Run Full Platform

```bash
cd data-platform
../start.sh
```

### 2. Run with Docker

```bash
cd data-platform/docker
docker-compose up -d
```

### 3. Use API

```bash
# Sales data
curl http://localhost:8000/api/sales

# Products
curl http://localhost:8000/api/products

# KPIs
curl http://localhost:8000/api/kpis/category
```

---

## 🐳 Docker Services

| Service | Port | Description |
|---------|------|-------------|
| PostgreSQL | 5432 | Database |
| API | 8000 | REST API |
| Dashboard | 8888 | Web UI |

---

## 📊 Data Flow

```
┌─────────┐    ┌─────────┐    ┌──────────┐    ┌─────────┐
│   API   │───▶│  Raw   │───▶│ Bronze  │───▶│ Silver │
└─────────┘    └─────────┘    └──────────┘    └─────────┘
                                              │
                                         ┌─────▼─────┐
                                         │   Gold   │ (KPIs)
                                         └──────────┘
                                              │
                                         ┌──────▼──────┐
                                         │PostgreSQL │
                                         └──────────┘
                                              │
                                         ┌──────▼──────┐
                                         │Dashboard  │
                                         └──────────┘
```

---

## 🔧 API Endpoints

| Method | Endpoint | Description |
|--------|---------|------------|
| GET | /api/sales | Sales data |
| GET | /api/products | Products |
| GET | /api/customers | Customers |
| GET | /api/kpis/category | KPIs by category |
| GET | /api/kpis/date | KPIs by date |
| GET | /api/kpis/product | Top products |
| GET | /api/kpis/customer | Top customers |

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

**Built with** ❤️ using Python stdlib + production tools