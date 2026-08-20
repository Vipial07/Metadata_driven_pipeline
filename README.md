# 🏦 Metadata-Driven Data Pipeline — Neo Bank Analytics

A fully **metadata-driven, config-based ETL framework** built on **Databricks + Delta Lake (Unity Catalog)** that ingests data from multiple heterogeneous sources (Oracle DB, CSV/Blob files), processes it through a **Bronze → Silver → Gold** medallion architecture, and powers an executive analytics dashboard for a fictional Neo Bank.

Instead of writing a separate notebook/pipeline for every table, this project uses **metadata tables** to describe *what* to load, *how* to load it (full / incremental / merge), and *where* it comes from — so a single set of generic notebooks can onboard new data sources by adding rows to a config table, not by writing new code.

---

## 📐 Architecture

```
                 ┌───────────────────────┐
 Oracle DB  ───▶ │                       │
 (Customers,     │   Source → Bronze     │──▶  Bronze Layer (Raw / As-is)
 Accounts,       │   (m_source_to_bronze)│
 Transactions,   │                       │
 Branches)       └───────────────────────┘
                             │
 Blob / CSV  ───▶ (Autoloader / cloudFiles)
 (Credit Bureau,
 Payment Gateway)
                             ▼
                 ┌───────────────────────┐
                 │   Bronze → Silver     │──▶  Silver Layer (Cleansed / Conformed)
                 │  (m_bronze_to_silver) │
                 └───────────────────────┘
                             │
                             ▼
                 ┌───────────────────────┐
                 │   Silver → Gold       │──▶  Gold Layer (Business KPIs)
                 │ (silver_to_gold_driver│
                 │   _code + KPI SQL)    │
                 └───────────────────────┘
                             │
                             ▼
                 ┌───────────────────────┐
                 │  Power BI–style       │
                 │  Databricks Dashboard │
                 │  (Executive View)     │
                 └───────────────────────┘
```

Every stage reads its configuration (source system, columns, load type, primary key, watermark column, target schema) from a central **metadata catalog** rather than hardcoding it — enabling the same generic notebooks to process *any* table in the `banking.metadata.tables` config.

---

## 📂 Repository Structure

```
Metadata_driven_pipeline/
│
├── Meatada_Setup/
│   └── Setup_Metadata.sql          # Creates the metadata catalog/schema and all control tables
│
├── Source_file/
│   ├── 01_Oracle/                  # DDL + sample INSERT scripts for Oracle source tables
│   └── 02_Flat_file/               # Sample CSV files simulating Blob-based sources
│
├── Source_to_bronze/
│   ├── get_table_id_run_id.py      # Fetches active table_ids + generates a new run_id
│   └── m_source_to_bronze.py       # Generic notebook: reads metadata → connects to source → loads Bronze
│
├── bronze_to_silver/
│   └── m_bronze_to_silver.py       # Generic notebook: Bronze → Silver (Full/Merge/Append via metadata)
│
├── silver_to_gold/
│   ├── silver_to_gold_driver_code.py       # Driver that loops through configured KPI notebooks
│   ├── branch_performance.py.sql
│   ├── customer_360.py.sql
│   ├── daily_bank_kpi.py
│   ├── risk_customer_summary.py.sql
│   └── transaction_channel_summary.py.sql
│
├── Dashboard/
│   └── Neo Bank Executive Dashboard.lvdash.json   # Databricks Lakeview dashboard definition
│
└── README.md
```

---

## 🧠 Metadata Framework

All control logic lives in the `banking.metadata` schema, created by `Setup_Metadata.sql`:

| Table | Purpose |
|---|---|
| `tables` | Master registry of every source table — source system, schema/column list, source path, target Bronze/Silver/Gold schema, active flag, load order |
| `table_parameters` | Per-table settings: `load_type` (`FULL` / `MERGE` / `APPEND`), `primary_key`, `watermark_column` |
| `table_watermarks` | Tracks the last processed watermark value per table, for incremental loads |
| `pipeline_runs` | Execution log — run id, layer, start/end time, duration, record counts, status |
| `gold_layer_kpi` | Registry of Gold-layer KPI notebooks to execute and their paths |
| `run_execution_details` | Overall run-level start/end time and duration tracking |

### Currently onboarded tables

| Table | Source | Load Type | Watermark |
|---|---|---|---|
| customers | Oracle DB | MERGE | `updated_at` |
| accounts | Oracle DB | MERGE | `updated_at` |
| transactions | Oracle DB | APPEND | `txn_timestamp` |
| branches | Oracle DB | FULL | — |
| credit_bureau_reports | Blob (CSV) | MERGE | `bureau_pull_date` |
| payment_gateway_logs | Blob (CSV) | APPEND | `processed_timestamp` |

Adding a new source **only requires inserting new rows** into `metadata.tables` and `metadata.table_parameters` — no new notebook code needed.

---

## ⚙️ Pipeline Stages

### 1. Source → Bronze (`Source_to_bronze/`)
- `get_table_id_run_id.py` pulls all `active_flag = true` table ids from the metadata catalog and generates the next `run_id`, passed downstream via Databricks job task values.
- `m_source_to_bronze.py` is a **parameterized notebook** (accepts `table_id`, `run_id` as widgets) that:
  - Joins `tables`, `table_parameters`, and `table_watermarks` to build a runtime config for the given table.
  - Connects to **Oracle** (via `oracledb`) for relational sources, applying `FULL`, `MERGE`, or watermark-based incremental `SELECT` queries.
  - Or reads **CSV files via Databricks Auto Loader** (`cloudFiles`) for Blob-based sources, using schema inference/checkpointing.
  - Writes to the Bronze Delta table using `overwrite` (Full), `MERGE` (upsert), or `append`, depending on `load_type`.
  - Logs the run outcome (record count, duration, status) into `metadata.pipeline_runs`.

### 2. Bronze → Silver (`bronze_to_silver/`)
- `m_bronze_to_silver.py` reads the same metadata config, adds `insert_timestamp` / `update_timestamp` audit columns, and:
  - **FULL**: overwrites the Silver table.
  - **MERGE**: filters Bronze data past the last watermark and merges into Silver on the primary key.
  - **APPEND**: filters past the last watermark and appends to Silver.
  - Updates `metadata.table_watermarks` with the new max watermark value after a successful load.
  - Logs run metrics into `metadata.pipeline_runs`.

### 3. Silver → Gold (`silver_to_gold/`)
- `silver_to_gold_driver_code.py` reads the `gold_layer_kpi` registry and orchestrates each KPI notebook via `dbutils.notebook.run(...)`, logging status/duration/record counts.
- KPI notebooks build curated, business-ready Gold tables:
  - **`customer_360`** — a 360° customer view combining accounts, transactions, credit bureau data, and a computed value segment (`HIGH_VALUE` / `MEDIUM_VALUE` / `LOW_VALUE`).
  - **`branch_performance`** — branch-level rollups of customers, accounts, and balances.
  - **`daily_bank_kpi`** — daily transaction volume/value plus customer, account, balance, and risk metrics — a single source of truth for reporting.
  - **`risk_customer_summary`** — high-risk customers (by credit bureau risk grade) with active, KYC-verified accounts.
  - **`transaction_channel_summary`** — transaction counts/value by channel for successful transactions.

### 4. Executive Dashboard (`Dashboard/`)
A Databricks Lakeview dashboard (`Neo Bank Executive Dashboard.lvdash.json`) built on top of the Gold tables, covering:
- Total customers, deposits, and transactions
- High-risk customer counts
- Transaction summary by channel and business date
- Customer segmentation and risk-grade breakdown
- Branch-wise customer and deposit distribution
- Top customers by value

---

## 🛠️ Tech Stack

- **Compute / Platform:** Databricks (Notebooks, Jobs, Auto Loader, Lakeview Dashboards)
- **Storage Format:** Delta Lake, governed via **Unity Catalog**
- **Processing:** PySpark, Spark SQL
- **Sources:** Oracle DB (via `oracledb` Python driver), CSV files (Volumes/Blob storage)
- **Orchestration:** Databricks Jobs (task values passed between `get_table_id_run_id` → `source_to_bronze` → `bronze_to_silver` → `silver_to_gold_driver_code`)
- **Config/Secrets:** `python-dotenv` for local Oracle credentials

---

## 🚀 Getting Started

1. **Set up the metadata catalog** — run `Meatada_Setup/Setup_Metadata.sql` in a Databricks workspace with Unity Catalog enabled. This creates the `banking` catalog, `metadata` schema, and all control tables described above, plus sample rows for the six onboarded tables.
2. **Load sample source data**
   - Run the DDL and insert scripts under `Source_file/01_Oracle/` against an Oracle database to create and populate `customers`, `accounts`, `transactions`, and `branches`.
   - Upload the CSV files under `Source_file/02_Flat_file/` to the Blob/Volume path referenced in `metadata.tables` (`/Volumes/banking/source/blob_file/...`).
3. **Configure Oracle connectivity** — set Oracle DSN/user/password (e.g. via a `.env` file loaded with `python-dotenv`) so `m_source_to_bronze.py` can connect.
4. **Orchestrate as a Databricks Job** with tasks in this order, wiring `table_id`/`run_id` as task-value parameters:
   1. `get_table_id_run_id.py`
   2. `m_source_to_bronze.py` (looped per `table_id`)
   3. `m_bronze_to_silver.py` (looped per `table_id`)
   4. `silver_to_gold_driver_code.py`
5. **Publish the dashboard** — import `Dashboard/Neo Bank Executive Dashboard.lvdash.json` into Databricks Lakeview and point it at the `banking.gold` tables.

---

## 📈 Future Enhancements

- Add SQL Server as a live source connector alongside Oracle and Blob
- Parameterize schema/target paths further to remove any remaining hardcoded catalog/schema names
- Add data quality checks and alerting into `pipeline_runs`
- Automate the Databricks Job/task orchestration as Infrastructure-as-Code (e.g. Databricks Asset Bundles)

---

## 👤 Author

**Vipin Gupta** — Data Engineer