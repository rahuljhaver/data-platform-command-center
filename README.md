# Retail Data Platform Command Center

A Databricks App that provides a web-based dashboard for monitoring a retail medallion data pipeline. Built with Flask and the Databricks SDK, it gives real-time visibility into pipeline status, data sources, bronze/silver/gold table lineage, data quality checks, and update history.

## What It Does

- **Pipeline Overview** — Live status, state, and configuration of the monitored Spark Declarative Pipeline (`Pipeline-example1`)
- **Data Sources** — Tracks four CSV sources with CDC events: Customer, Sales, Product, and Store, including ingestion cadence and SLA
- **Medallion Architecture** — Visualizes the full bronze → silver → gold flow:
  - **Bronze**: Streaming tables ingesting raw CDC data from volumes
  - **Silver**: Auto CDC (SCD1) tables with quarantine for bad records
  - **Gold**: Materialized views for daily sales, sales by customer, store, and product
- **Data Quality** — Monitors DQ checks (null, empty, non-positive values) with quarantine routing
- **Update History** — Lists recent pipeline updates with state, type, and timestamps

## Architecture

```
CSV Sources (Volumes/retail/raw/data/)
    │
    ▼
Bronze Layer (Streaming Tables with DQ)
    │
    ▼
Silver Layer (Auto CDC / SCD1 + Quarantine)
    │
    ▼
Gold Layer (Materialized Views — Aggregations)
```

## Project Structure

| File | Description |
| --- | --- |
| `app.py` | Flask application with pipeline metadata, live SDK calls, and HTML dashboard |
| `app.yaml` | Databricks App configuration (entrypoint command) |
| `requirements.txt` | Python dependencies (Flask, Databricks SDK) |

## Dependencies

- `flask>=3.0`
- `databricks-sdk>=0.29`

## How to Run

This app is designed to run as a **Databricks App**. The `app.yaml` file configures the entrypoint:

```yaml
command:
  - python
  - app.py
```

When deployed, the app uses the Databricks SDK `WorkspaceClient` to fetch live pipeline data. If the SDK is unavailable, it falls back to static metadata embedded in the source code.

## Monitored Pipeline

- **Pipeline ID**: `dbdd8d12-1af2-4dad-a6c0-962152e7e2af`
- **Pipeline Name**: Pipeline-example1
- **Catalog**: `retail`
- **Schemas**: `retail.bronze`, `retail.silver`, `retail.gold`
