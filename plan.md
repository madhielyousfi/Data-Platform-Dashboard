# Data Platform — Implementation Plan

## Stack Summary

| Layer | Technology |
|---|---|
| **Sources** | Kaggle CSV files (downloaded manually) |
| **Ingestion** | Python (pandas) — batch |
| **Storage** | `/data/` directory with zone structure |
| **Warehouse** | SQLite (local file)
| **Processing** | Python + pandas (bronze→silver→gold) |
| **Orchestration** | Apache Airflow |
| **Visualization** | Metabase (free, open) |

---

## Project Structure

```
data-platform/
├── data/
│   ├── raw/          # Bronze - ingested CSVs
│   ├── staging/      # Silver - cleaned, normalized
│   └── gold/         # Gold - business KPIs
├── ingestion/        # Scripts to load raw data
├── processing/       # Bronze → Silver → Gold scripts
├── warehouse/        # SQL schema + load scripts
├── dags/             # Airflow DAGs
├── config/           # Config YAML files
├── dashboards/       # Metabase export
└── docker/           # (minimal, just for Metabase)
```

---

## Tech Stack (Free + Realistic)

- **Ingestion**: Python + pandas (CSV/Parquet)
- **Storage**: Local filesystem + SQLite
- **Processing**: pandas (small-to-medium data)
- **Orchestration**: Apache Airflow (already installed)
- **Visualization**: Metabase (Docker) or Power BI
- **Containerization**: Docker (optional)

---

## Data Flow

### Step 1 — Ingestion
- Download CSV from Kaggle
- Store raw in `/data/raw/`
- Output: `data/raw/*.csv`

### Step 2 — Processing (Bronze → Silver → Gold)
- **Bronze**: Raw CSV — no changes
- **Silver**: Cleaned — remove nulls, normalize schema, add audit columns
- **Gold**: Aggregated KPIs — totals, top N, trends

### Step 3 — Warehouse
- Load gold into SQLite
- Schema: Star schema (fact + dimension tables)

### Step 4 — Orchestration (Airflow DAG)
- DAG: `etl_pipeline`
- Tasks: `extract` → `load_raw` → `transform` → `load_warehouse`
- Schedule: Daily

### Step 5 — Visualization
- Connect Metabase/Power BI to SQLite
- Dashboards: Sales over time, top products, KPIs

---

## Recommended Dataset

**Superstore Sales** (Kaggle)
- URL: https://www.kaggle.com/datasets/vivek468/superstore-dataset-final
- Contains: Orders, Customers, Products, Geography
- Covers all modeling needs

---

## Execution Phases

| Phase | Description | Steps | Effort |
|---|---|---|---|
| **P0** | Foundation | Directory structure + sample data | 30 min |
| **P1** | Ingestion | SQLite schema + load_raw.py | 1-2 hrs |
| **P2** | Processing | Bronze → Silver → Gold scripts | 1-2 hrs |
| **P3** | Orchestration | Airflow DAG wiring | 1 hr |
| **P4** | Visualization | Metabase dashboards | 1 hr |

---

## Key Principles

- ✅ **Reusability**: Config-driven pipelines
- ✅ **Scalability**: Partitioned data (by date)
- ✅ **Observability**: Logs + error handling
- ✅ **Data Quality**: Validate schema, check nulls

---

## Next Steps

1. Create directory structure
2. Download Superstore dataset to `data/raw/`
3. Write `config/sources.yaml`
4. Build ingestion script