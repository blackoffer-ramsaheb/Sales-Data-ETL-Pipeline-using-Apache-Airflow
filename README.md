# Apache Airflow — Sales ETL Pipeline (Learning Project)

A **medium-level** Apache Airflow ETL project demonstrating core orchestration concepts through a realistic sales data pipeline. Built with Docker Compose, the TaskFlow API, SQLite, and clean separation of business logic from orchestration code.

---

## Project Structure

```
Apache Airflow/
│
├── dags/
│   └── sales_etl_pipeline.py   ← DAG definition (TaskFlow API)
│
├── scripts/                     ← Pure Python business logic (no Airflow imports)
│   ├── extract.py               ← Read raw CSV → list of dicts
│   ├── validate.py              ← Data quality checks
│   ├── transform.py             ← Type-cast, compute Total, write processed CSV
│   ├── load.py                  ← Upsert into SQLite
│   └── report.py                ← Query DB and write summary.txt
│
├── data/
│   ├── raw/
│   │   └── sales.csv            ← Source data (with intentional quality issues)
│   ├── processed/
│   │   └── sales_clean.csv      ← Output of transform step
│   └── reports/
│       └── summary.txt          ← Output of report step
│
├── database/
│   └── sales.db                 ← SQLite DB (created on first DAG run)
│
├── config/                      ← Airflow config (airflow.cfg auto-generated)
├── logs/                        ← Airflow task logs
├── plugins/                     ← Custom Airflow plugins (currently empty)
├── docker-compose.yaml          ← Full Airflow cluster (Celery + Redis + Postgres)
├── .env                         ← Environment variables
└── requirements.txt             ← Python dependencies
```

---

## Pipeline Overview

```
[Extract CSV] → [Validate Data] → [Transform Data] → [Load SQLite] → [Generate Report]
```

| Step | Task ID | What it does |
|---|---|---|
| 1 | `extract_sales_csv` | Read `data/raw/sales.csv` → list of row dicts |
| 2 | `validate_data` | Drop rows with missing fields, duplicate IDs, bad quantity/price |
| 3 | `transform_data` | Cast types, compute `total`, normalise strings, write `sales_clean.csv` |
| 4 | `load_to_sqlite` | Upsert into `sales_orders` table in `sales.db` |
| 5 | `generate_summary_report` | Query DB, write formatted `summary.txt` |

Data flows between tasks via **implicit XCom** — each `@task` returns a value that is automatically passed as an argument to the next task.

---

## Airflow Concepts Demonstrated

| Concept | Where |
|---|---|
| `@dag` / `@task` decorators (TaskFlow API) | `sales_etl_pipeline.py` |
| Implicit XCom (return values as arguments) | All tasks |
| `retries` & `retry_delay` | `DEFAULT_ARGS` in DAG |
| `catchup=False` | `@dag(catchup=False)` |
| `max_active_runs=1` | Prevent parallel DAG runs |
| `tags` | `["etl", "sales", "sqlite", "learning"]` |
| `schedule="@daily"` | Daily scheduling |
| Business logic in `scripts/` (not in DAG) | All 5 script files |
| Idempotent loads (`INSERT OR REPLACE`) | `load.py` |

---

## Quick Start

### 1. Prerequisites
- Docker Desktop (with at least **4 GB RAM** allocated to Docker)
- Docker Compose v2+

### 2. Generate a Fernet Key
```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
Copy the output and paste it as the value of `FERNET_KEY` in `.env`.

### 3. Initialise & Start Airflow
```powershell
# From the project root:
docker compose up airflow-init
docker compose up -d
```

### 4. Access the Airflow UI
Open **http://localhost:8080** in your browser.  
Login: `airflow` / `airflow`

### 5. Enable & Trigger the DAG
1. Search for **`sales_etl_pipeline`** in the DAG list.
2. Toggle it **ON** (unpause).
3. Click the ▶ **Trigger** button to run it immediately.

### 6. Check Outputs
After the DAG completes:
- **Processed CSV**: `data/processed/sales_clean.csv`
- **SQLite DB**: `database/sales.db`
- **Summary Report**: `data/reports/summary.txt`

### 7. Tear Down
```powershell
docker compose down --volumes
```

---

## Sample Data Quality Issues in `sales.csv`

The raw CSV intentionally contains the following issues for the validation step to catch:

| Row | Issue |
|---|---|
| ORD002 (2nd occurrence) | Duplicate order ID |
| ORD011 | Missing quantity AND negative price |
| ORD014 | Missing product name |
| ORD016 | Negative quantity (-2) |

---

## Validation Rules

| Rule | Column | Check |
|---|---|---|
| No missing values | All 6 columns | `value is None or blank` |
| No duplicate IDs | `order_id` | Seen set |
| Valid quantity | `quantity` | Must be `int > 0` |
| Valid price | `price` | Must be `float > 0` |

---

## Extending the Pipeline

| Idea | Where to change |
|---|---|
| Add more validation rules | `scripts/validate.py` |
| Add more transformations | `scripts/transform.py` |
| Switch to PostgreSQL | `scripts/load.py` — replace `sqlite3` with `psycopg2` |
| Send report via email | Add a 6th `@task` in the DAG |
| Load from S3 instead of local CSV | `scripts/extract.py` — use `boto3` |
| Add unit tests | Create `tests/` directory |
