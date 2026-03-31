# Oil Price & Geopolitical Events — Data Pipeline

**Module 2 | NTU | March 2026**

End-to-end data pipeline analysing the relationship between crude oil prices and geopolitical events (2010–2026).

---

## Pipeline Architecture

```
CSV Files
   ↓
DuckDB  (local ingestion & validation)
   ↓
Meltano  (Extract & Load via tap-csv → target-bigquery)
   ↓
Google BigQuery  (cloud data warehouse)
   ↓
dbt  (staging views + mart transformation)
   ↓
Dagster  (orchestration & monitoring)
   ↓
Power BI / Jupyter  (dashboards & EDA)
```

---

## Project Structure

```
oil-price-pipeline/
├── data/
│   └── oil_pipeline.duckdb          # Local DuckDB database
├── ingestion/
│   └── load_to_duckdb.py            # Step 1: CSV → DuckDB ingestion script
├── .env                             # Local environment variables (not committed)
├── .env.example                     # Environment variable template
├── requirements.txt                 # Python dependencies
└── README.md
```

**WSL components (~/meltano-ingestion/):**
```
meltano-ingestion/
├── meltano.yml                      # Meltano project config
├── csv_files_definition.json        # tap-csv source definitions
├── oil_eda.ipynb                    # Jupyter EDA notebook
├── oil_pipeline_report.html         # Full project report
├── oil_pipeline_presentation.pptx   # Presentation slides
├── oil_pipeline_dbt/                # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   ├── stg_oil_prices.sql
│   │   │   └── stg_geo_events.sql
│   │   └── marts/
│   │       └── mart_oil_geopolitics.sql
│   └── dbt_project.yml
└── oil_pipeline_dagster/            # Dagster project
    └── oil_pipeline_dagster/
        ├── assets.py
        └── definitions.py
```

---

## Dataset

| File | Rows | Period | Description |
|------|------|--------|-------------|
| `oil_geopolitics_dataset_2010_2026.csv` | 4,047 | 2010–2026 | Daily oil prices, VIX, DXY, GPR Index, returns, volatility |
| `geopolitical_events_timeline.csv` | 35 | 2010–2026 | Geopolitical events with type, description, severity (1–5) |

**Source location (Windows):**
```
C:/Users/rakhi/OneDrive/Documents/NTU/Module 2/Oil Price CSV/
```

**Key variables:**

| Variable | Description |
|----------|-------------|
| `brent_price` | Brent crude spot price (USD/barrel) |
| `wti_price` | WTI crude spot price (USD/barrel) |
| `dxy_index` | US Dollar Index — currency strength |
| `vix` | CBOE Volatility Index — market fear |
| `gpr_index` | Geopolitical Risk Index |
| `brent_return` | Daily log return of Brent price |
| `wti_return` | Daily log return of WTI price |
| `event_type` | Geopolitical event category |
| `event_severity` | Event severity score (1–5) |

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.11 | Via conda in WSL — NOT Windows Python 3.13 |
| Meltano | 4.1.2 | Run in WSL only |
| dbt-bigquery | 1.11.7 | Installed in meltano-env |
| Dagster | 1.12.19 | Installed in meltano-env |
| Jupyter Lab | 4.5.6 | Installed in meltano-env |
| DuckDB | >=1.1.0 | Windows side for Step 1 |
| Google Cloud SDK | Latest | For ADC authentication |
| WSL Ubuntu | Any | Required for Meltano and dbt |

---

## Setup

### 1. Environment (WSL)

```bash
# Create conda environment with Python 3.11
conda create -n meltano-env python=3.11
conda activate meltano-env

# Install all tools
pip install meltano dbt-bigquery dagster dagster-webserver
pip install jupyter pandas pandas-gbq matplotlib seaborn
```

### 2. GCP Authentication

JSON key creation is blocked by organisation policy. Use ADC instead:

```bash
# On Windows (PowerShell or Cloud SDK shell)
gcloud auth application-default login

# Copy credentials to WSL
cp /mnt/c/Users/rakhi/AppData/Roaming/gcloud/application_default_credentials.json \
   ~/.config/gcloud/application_default_credentials.json
```

### 3. Environment Variables

Copy `.env.example` to `.env` and update paths:

```bash
cp .env.example .env
```

```env
DUCKDB_PATH=data/oil_pipeline.duckdb
GCP_PROJECT_ID=module-2-project-490315
GCP_DATASET=oil_pipeline_raw
CSV_OIL_PRICES=C:/Users/rakhi/OneDrive/Documents/NTU/Module 2/Oil Price CSV/oil_geopolitics_dataset_2010_2026.csv
CSV_GEO_EVENTS=C:/Users/rakhi/OneDrive/Documents/NTU/Module 2/Oil Price CSV/geopolitical_events_timeline.csv
```

---

## Running the Pipeline

### Step 1 — DuckDB Ingestion (Windows)

```bash
cd "C:/Users/rakhi/OneDrive/Documents/NTU/Module 2/Project Sample 2/oil-price-pipeline"
python ingestion/load_to_duckdb.py
```

Expected output:
```
raw_oil_prices: 4,047 rows loaded
raw_geo_events: 35 rows loaded
DuckDB ingestion complete.
```

### Step 2 — Meltano Extract & Load (WSL)

```bash
conda activate meltano-env
cd ~/meltano-ingestion
meltano run tap-csv target-bigquery
```

Expected output:
```
record_count: 4047  (raw_oil_prices)
record_count: 35    (raw_geo_events)
Run completed
```

### Step 3 — dbt Transformations (WSL)

```bash
cd ~/meltano-ingestion/oil_pipeline_dbt
dbt run
```

Expected output:
```
1 of 3 OK  stg_geo_events
2 of 3 OK  stg_oil_prices
3 of 3 OK  mart_oil_geopolitics
PASS=3 ERROR=0
```

### Step 4 — dbt Tests (WSL)

```bash
dbt test
```

Expected output:
```
PASS=14  WARN=0  ERROR=0  TOTAL=14
```

### Step 5 — Dagster Orchestration (WSL)

```bash
cd ~/meltano-ingestion/oil_pipeline_dagster
dagster dev
```

Open browser: [http://localhost:3000](http://localhost:3000)

Assets in order: `meltano_extract_load` → `dbt_transformations` → `dbt_tests`

Click **Materialize all** to run the full pipeline.

### Step 6 — Jupyter EDA (WSL)

```bash
cd ~/meltano-ingestion
jupyter lab --no-browser --port=8888
```

Open browser with the token URL shown in terminal. Open `oil_eda.ipynb` and run all cells.

Charts produced:
- Oil prices over time with geopolitical event markers
- VIX vs Brent Price scatter plot
- Correlation heatmap of all variables

### Step 7 — Power BI

1. Open Power BI Desktop
2. Get Data → Google BigQuery
3. Sign in with Google account (same account as GCP)
4. Project: `module-2-project-490315` → dataset: `oil_pipeline_raw`
5. Load: `mart_oil_geopolitics`, `stg_oil_prices`, `stg_geo_events`

---

## BigQuery Schema

### GCP Config

| Setting | Value |
|---------|-------|
| Project ID | `module-2-project-490315` |
| Dataset | `oil_pipeline_raw` |
| Location | US |
| Auth | Application Default Credentials (ADC) |

### Tables & Views

| Name | Type | Rows | Description |
|------|------|------|-------------|
| `raw_oil_prices` | Table | 4,047 | Raw oil prices loaded by Meltano |
| `raw_geo_events` | Table | 35 | Raw geopolitical events loaded by Meltano |
| `stg_oil_prices` | dbt View | 4,047 | Cleaned — date cast to DATE, prices to FLOAT64 |
| `stg_geo_events` | dbt View | 35 | Cleaned — date cast to DATE, severity to INT64 |
| `mart_oil_geopolitics` | dbt View | 4,047 | Joined oil prices + events on date |

---

## dbt Data Quality Tests

| Model | Column | Test | Result |
|-------|--------|------|--------|
| stg_oil_prices | date | not_null, unique | PASS |
| stg_oil_prices | brent_price | not_null | PASS |
| stg_oil_prices | wti_price | not_null | PASS |
| stg_oil_prices | vix | not_null | PASS |
| stg_oil_prices | gpr_index | not_null | PASS |
| stg_geo_events | date | not_null, unique | PASS |
| stg_geo_events | event_type | not_null | PASS |
| stg_geo_events | event_severity | not_null | PASS |
| mart_oil_geopolitics | date | not_null, unique | PASS |
| mart_oil_geopolitics | brent_price | not_null | PASS |
| mart_oil_geopolitics | wti_price | not_null | PASS |

**Total: 14/14 PASS**

---

## Meltano Configuration

**meltano.yml** (key settings):

```yaml
plugins:
  extractors:
  - name: tap-csv
    variant: meltanolabs
    config:
      csv_files_definition: csv_files_definition.json
  loaders:
  - name: target-bigquery
    variant: z3z1ma
    config:
      project: module-2-project-490315
      dataset: oil_pipeline_raw
      location: US
      method: storage_write_api
      denormalized: true
      overwrite: true
```

---

## Key Findings

| Finding | Summary |
|---------|---------|
| Event-driven volatility | Every major price dislocation (>20%) linked to a geopolitical event, pandemic, or OPEC policy shift |
| Brent-WTI correlation | ~0.99 correlation — move almost identically; spread widens during Middle East disruptions |
| VIX-Oil relationship | Non-linear — high VIX usually means lower oil, except supply shock events (e.g. Ukraine 2022) |
| GPR leads price spikes | GPR >200 preceded 10–25% Brent price increase within 30–60 days |
| DXY inverse relationship | Stronger dollar suppresses oil prices (correlation -0.40 to -0.55) |
| Volatility clustering | ±2% returns in calm periods vs ±10% during geopolitical shock periods |

---

## Validation Results

```
DuckDB     raw_oil_prices: 4047 rows     PASS
DuckDB     raw_geo_events: 35 rows       PASS
Meltano    version 4.1.2                 PASS
Meltano    tap-csv reachable             PASS
BigQuery   raw tables accessible         PASS
BigQuery   dbt views accessible          PASS
dbt        connection debug              PASS
GCP        ADC credentials present       PASS

TOTAL: 7/7 PASS
```

---

## Known Issues & Workarounds

| Issue | Workaround |
|-------|-----------|
| ARM64 + Python 3.13 build failures | Use Python 3.11 in WSL conda env |
| WSL disconnects on OneDrive path | Run all WSL commands from `~/meltano-ingestion/` |
| GCP JSON key blocked by org policy | Use ADC via `gcloud auth application-default login` |
| `pkg_resources` not found in venv | `pip install setuptools==69.5.1` in target-bigquery venv |
| Bash heredoc corrupted on paste | Use `python3 - << 'PYEOF'` inline scripts for file writing |
| BigQuery columns stored as JSON blob | Add `denormalized: true` to target-bigquery config |

---

## Tech Stack

| Layer | Tool | Version |
|-------|------|---------|
| Local warehouse | DuckDB | >=1.1.0 |
| Extract & Load | Meltano + tap-csv + target-bigquery | 4.1.2 |
| Cloud warehouse | Google BigQuery | - |
| Transformation | dbt-bigquery | 1.11.7 |
| Orchestration | Dagster | 1.12.19 |
| EDA | Jupyter Lab + pandas + matplotlib + seaborn | 4.5.6 |
| Dashboard | Power BI Desktop | - |
| Environment | WSL Ubuntu + conda (Python 3.11) | - |
| Auth | Google Cloud ADC | - |
