# Retail Orders Lakehouse — PySpark + Delta Lake

An interview-ready end-to-end batch ETL project demonstrating a **Bronze → Silver → Gold** lakehouse on PySpark and Delta Lake.

## What this project does

1. Generates a reproducible retail orders CSV with valid records and deliberate data-quality issues.
2. Ingests raw CSV files into a Bronze Delta table with ingestion metadata.
3. Cleans and standardizes records in Silver: trims strings, normalizes case, parses dates/numbers, validates required fields, quarantines invalid rows, and deduplicates by `order_id`.
4. Enriches orders with customer and product dimensions using joins.
5. Builds Gold business aggregates: daily sales, category performance, and customer lifetime value.
6. Runs data-quality checks and writes a JSON quality report.

## Architecture

```text
CSV source files
      |
      v
BRONZE (Delta)  -- raw values + source_file + ingestion_timestamp + batch_id
      |
      v
SILVER (Delta)  -- typed, standardized, deduplicated, validated orders
      |                         \
      |                          +--> QUARANTINE (Delta): rejected rows + reason
      |
      +---- customers dimension (CSV -> Delta)
      +---- products dimension  (CSV -> Delta)
      |
      v
GOLD (Delta)
  +-- daily_sales
  +-- category_performance
  +-- customer_lifetime_value
      |
      v
quality_report.json + Spark output tables
```

## Tech stack

- Python 3.10 or 3.11
- Apache Spark / PySpark 3.5.3
- Delta Lake 3.2.1
- pytest

## Repository structure

```text
pyspark-delta-lake-etl/
├── data/source/                 # Generated CSV inputs
├── docs/                        # Architecture and interview notes
├── src/pipeline/
│   ├── config.py                # Paths and Spark configuration
│   ├── data_generator.py        # Reproducible source-data generator
│   ├── quality.py               # Data-quality rules and report
│   ├── transformations.py       # Bronze/Silver/Gold transformations
│   └── run_pipeline.py           # End-to-end orchestration
├── tests/                       # Unit tests for transformation logic
├── .github/workflows/ci.yml     # Lightweight CI checks
├── requirements.txt
└── run.py
```

## Quick start

> Use Java 8/11/17 (Java 17 recommended) and Python 3.10/3.11. Delta Lake is enabled through Spark session extensions in `config.py`.

### 1. Create an environment

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
# macOS/Linux
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Generate source data

```bash
python run.py --generate-only
```

This creates `data/source/orders.csv`, `customers.csv`, and `products.csv`.

### 3. Run the complete pipeline

```bash
python run.py
```

The default output is written to `data/lakehouse/` with Delta tables in `bronze/`, `silver/`, `quarantine/`, `dimensions/`, and `gold/`.

### 4. Run tests

```bash
pytest -q
```

## Data model

### orders.csv

| Column | Meaning |
|---|---|
| order_id | Business order key |
| customer_id | Customer foreign key |
| product_id | Product foreign key |
| order_date | Source date string (`yyyy-MM-dd`) |
| quantity | Units ordered |
| unit_price | Unit selling price |
| discount | Discount fraction (0 to 1) |
| status | Source order status |
| region | Sales region |

### Silver derived fields

- `gross_amount = quantity * unit_price`
- `discount_amount = gross_amount * discount`
- `net_amount = gross_amount - discount_amount`
- typed `order_date`, `quantity`, `unit_price`, `discount`
- normalized `status`, `region`

## Data-quality rules

- `order_id`, `customer_id`, `product_id`, and `order_date` must be present.
- `order_date` must parse successfully.
- `quantity` must be greater than zero.
- `unit_price` must be non-negative.
- `discount` must be between 0 and 1 inclusive.
- `order_id` must be unique in Silver; the latest source occurrence is retained.
- Valid orders are enriched by joining customer and product dimensions.
- Invalid rows are written to the quarantine Delta path with `rejection_reason`.

## Design choices worth explaining in an interview

- **Medallion architecture:** Bronze preserves source fidelity; Silver creates trusted, conformed data; Gold serves business use cases.
- **Idempotency:** Delta `overwrite` for this demonstration makes reruns deterministic. In production, use a stable business key and Delta `MERGE` for incremental upserts.
- **Deduplication:** `row_number` over `order_id`, ordered by ingestion timestamp, selects the latest record deterministically.
- **Partitioning:** Gold daily sales is partitioned by `order_year` and `order_month`. Avoid partitioning high-cardinality columns such as `customer_id`.
- **Join strategy:** dimensions are small enough to broadcast in this sample. Remove the broadcast hint if dimensions grow beyond safe executor memory.
- **Optimization:** use column pruning, predicate pushdown, sensible partition sizes, AQE, and Delta compaction (`OPTIMIZE`) in supported managed runtimes. Do not run `OPTIMIZE` blindly on local open-source Delta environments.
- **Data quality:** rejected records are retained with reasons rather than silently dropped.

## Production extensions

1. Replace local paths with ADLS Gen2 / cloud object storage.
2. Orchestrate with Azure Data Factory or Databricks Workflows.
3. Add a watermark and incremental source extraction.
4. Use Delta `MERGE` for idempotent incremental processing.
5. Add schema evolution policy, audit tables, structured logging, alerting, and Unity Catalog governance.
6. Add SCD Type 2 for changing customer/product dimensions.

## Important honesty note for your portfolio

This is a runnable local demonstration using generated synthetic data. Do not describe it as a production deployment or claim performance improvements unless you benchmark and measure them yourself. Explain the design and trade-offs confidently.

## Interview walkthrough

See [`docs/INTERVIEW_GUIDE.md`](docs/INTERVIEW_GUIDE.md) for a concise architecture walkthrough, trade-offs, and likely questions.
