import json
import os
import logging
from flask import Flask, render_template_string, jsonify

# ─── Configuration ──────────────────────────────────────────────────────────
PIPELINE_ID = "dbdd8d12-1af2-4dad-a6c0-962152e7e2af"
PIPELINE_NAME = "Pipeline-example1"

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Workspace Client ───────────────────────────────────────────────────────
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
except Exception as e:
    w = None
    logger.warning(f"WorkspaceClient init failed: {e}")

# ─── Static Metadata (from source code analysis of Pipeline-example1) ────────

SOURCES = [
    {"name": "Customer", "format": "CSV", "cadence": "Daily", "path": "/Volumes/retail/raw/data/customer",
     "description": "Customer master data with CDC events", "schema_evo": "addNewColumns", "trigger": "1 day",
     "sla": "Freshness within 1 day", "columns": ["customer_id", "customer_name", "email", "operation_type", "source_ingested_timestamp"]},
    {"name": "Sales", "format": "CSV", "cadence": "Every 5 min", "path": "/Volumes/retail/raw/data/sales",
     "description": "Sales transactions with CDC events", "schema_evo": "addNewColumns", "trigger": "5 minutes",
     "sla": "Near real-time sales visibility", "columns": ["sale_id", "product_id", "store_id", "customer_id", "quantity", "unit_price", "total_amount", "event_timestamp", "operation_type", "source_ingested_timestamp"]},
    {"name": "Product", "format": "CSV", "cadence": "Hourly", "path": "/Volumes/retail/raw/data/product",
     "description": "Product master data with CDC events", "schema_evo": "addNewColumns", "trigger": "1 hour",
     "sla": "Freshness within 1 hour", "columns": ["product_id", "product_name", "unit_price", "operation_type", "source_ingested_timestamp"]},
    {"name": "Store", "format": "CSV", "cadence": "Daily", "path": "/Volumes/retail/raw/data/store",
     "description": "Store master data with CDC events", "schema_evo": "addNewColumns", "trigger": "1 day",
     "sla": "Freshness within 1 day", "columns": ["store_id", "store_name", "operation_type", "source_ingested_timestamp"]},
]

BRONZE_TABLES = [
    {"name": "retail.bronze.customer", "source": "Customer", "type": "Streaming Table", "trigger": "1 day", "key": "customer_id", "dq": True},
    {"name": "retail.bronze.sales", "source": "Sales", "type": "Streaming Table", "trigger": "5 minutes", "key": "sale_id", "dq": True},
    {"name": "retail.bronze.product", "source": "Product", "type": "Streaming Table", "trigger": "1 hour", "key": "product_id", "dq": True},
    {"name": "retail.bronze.store", "source": "Store", "type": "Streaming Table", "trigger": "1 day", "key": "store_id", "dq": True},
]

SILVER_TABLES = [
    {"name": "retail.silver.customer", "source": "retail.bronze.customer", "type": "Auto CDC (SCD1)", "key": "customer_id", "quarantine": "retail.silver.customer_quarantine"},
    {"name": "retail.silver.sales", "source": "retail.bronze.sales", "type": "Auto CDC (SCD1)", "key": "sale_id", "quarantine": "retail.silver.sales_quarantine"},
    {"name": "retail.silver.product", "source": "retail.bronze.product", "type": "Auto CDC (SCD1)", "key": "product_id", "quarantine": "retail.silver.product_quarantine"},
    {"name": "retail.silver.store", "source": "retail.bronze.store", "type": "Auto CDC (SCD1)", "key": "store_id", "quarantine": "retail.silver.store_quarantine"},
]

QUARANTINE_TABLES = [
    {"name": "retail.silver.customer_quarantine", "source": "retail.bronze.customer"},
    {"name": "retail.silver.sales_quarantine", "source": "retail.bronze.sales"},
    {"name": "retail.silver.product_quarantine", "source": "retail.bronze.product"},
    {"name": "retail.silver.store_quarantine", "source": "retail.bronze.store"},
]

GOLD_TABLES = [
    {"name": "retail.gold.daily_sales", "source": "retail.silver.sales", "type": "Materialized View", "agg": "By sale_date", "metrics": "total_sales, transaction_count"},
    {"name": "retail.gold.sales_by_customer", "source": "retail.silver.sales", "type": "Materialized View", "agg": "By customer_id", "metrics": "total_sales, transaction_count"},
    {"name": "retail.gold.sales_by_store", "source": "retail.silver.sales", "type": "Materialized View", "agg": "By store_id", "metrics": "total_sales, transaction_count"},
    {"name": "retail.gold.sales_by_product", "source": "retail.silver.sales", "type": "Materialized View", "agg": "By product_id", "metrics": "total_sales, transaction_count"},
]

DQ_CHECKS = [
    {"entity": "Sales", "table": "retail.silver.sales", "quarantine": "retail.silver.sales_quarantine",
     "checks": ["sale_id is null", "sale_id is empty", "product_id is null", "store_id is null",
                "quantity is null", "quantity is non-positive", "unit_price is null", "unit_price is non-positive",
                "total_amount is null", "total_amount is non-positive"]},
    {"entity": "Customer", "table": "retail.silver.customer", "quarantine": "retail.silver.customer_quarantine",
     "checks": ["customer_id is null", "customer_id is empty", "customer_name is null", "email is null"]},
    {"entity": "Product", "table": "retail.silver.product", "quarantine": "retail.silver.product_quarantine",
     "checks": ["product_id is null", "product_id is empty", "product_name is null",
                "unit_price is null", "unit_price is non-positive"]},
    {"entity": "Store", "table": "retail.silver.store", "quarantine": "retail.silver.store_quarantine",
     "checks": ["store_id is null", "store_id is empty", "store_name is null"]},
]

FALLBACK_UPDATES = [
    {"update_id": "98a2932a-bb39-4f64-a6a4-ae933fdc5852", "state": "COMPLETED",
     "creation_time": "2026-09-26T18:30:07.529Z", "update_type": "FULL"},
]

# ─── Helper Functions ───────────────────────────────────────────────────────

def safe_val(val, default="N/A"):
    if val is None:
        return default
    if hasattr(val, 'value'):
        return val.value
    return str(val)

def safe_bool(val, default=True):
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ('true', '1', 'yes')
    return bool(val)

# ─── Live Data Functions ────────────────────────────────────────────────────

def get_pipeline_info():
    if not w:
        return None
    try:
        resp = w.pipelines.get(PIPELINE_ID)
        config = {}
        if hasattr(resp, 'configuration') and resp.configuration:
            config = resp.configuration if isinstance(resp.configuration, dict) else {}
        storage_catalog = "workspace"
        storage_schema = "default"
        if resp.storage:
            storage_catalog = safe_val(getattr(resp.storage, 'catalog', None), "workspace")
            storage_schema = safe_val(getattr(resp.storage, 'schema', None), "default")
        return {
            "name": safe_val(resp.name, PIPELINE_NAME),
            "state": safe_val(resp.state, "UNKNOWN"),
            "pipeline_type": safe_val(resp.pipeline_type, "WORKSPACE"),
            "creator": safe_val(resp.creator_user_name, "N/A"),
            "catalog": storage_catalog,
            "schema": storage_schema,
            "photon": safe_bool(config.get("photon", True)),
            "serverless": safe_bool(config.get("serverless", True)),
            "channel": config.get("channel", "CURRENT") if isinstance(config.get("channel", "CURRENT"), str) else "CURRENT",
        }
    except Exception as e:
        logger.warning(f"Failed to get pipeline info: {e}")
        return None

def get_pipeline_updates():
    if not w:
        return FALLBACK_UPDATES
    updates = []
    try:
        resp = w.pipelines.list_updates(PIPELINE_ID)
        raw = getattr(resp, 'updates', None)
        if raw:
            for u in raw[:20]:
                updates.append({
                    "update_id": safe_val(getattr(u, 'update_id', None), "N/A"),
                    "state": safe_val(getattr(u, 'state', None), "UNKNOWN"),
                    "creation_time": safe_val(getattr(u, 'creation_time', None), "N/A"),
                    "update_type": safe_val(getattr(u, 'update_type', None), "N/A"),
                })
    except Exception as e:
        logger.warning(f"list_updates failed: {e}")
    if not updates:
        try:
            resp = w.pipelines.get(PIPELINE_ID)
            raw = getattr(resp, 'latest_updates', None)
            if raw:
                for u in raw[:20]:
                    updates.append({
                        "update_id": safe_val(getattr(u, 'update_id', None), "N/A"),
                        "state": safe_val(getattr(u, 'state', None), "UNKNOWN"),
                        "creation_time": safe_val(getattr(u, 'creation_time', None), "N/A"),
                        "update_type": safe_val(getattr(u, 'update_type', None), "N/A"),
                    })
        except Exception as e:
            logger.warning(f"latest_updates failed: {e}")
    return updates if updates else FALLBACK_UPDATES

# ─── HTML Template ──────────────────────────────────────────────────────────

TEMPLATE = r'''
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Data Platform Command Center</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
:root {
    --sidebar-bg: #1a1a2e; --sidebar-hover: rgba(255,255,255,0.06); --sidebar-active: #ff3621;
    --content-bg: #f5f5f7; --card-bg: #ffffff;
    --text-primary: #1a1a2e; --text-secondary: #6b6b76; --text-muted: #9b9ba5;
    --accent: #ff3621; --accent-blue: #0066ff;
    --success: #00a854; --warning: #ff9f1c; --error: #e53935;
    --border: #e8e8ed; --radius: 10px;
    --shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
    --shadow-lg: 0 4px 12px rgba(0,0,0,0.08);
}
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", sans-serif; background: var(--content-bg); color: var(--text-primary); font-size: 14px; line-height: 1.6; }
.app { display: flex; min-height: 100vh; }
.sidebar { width: 250px; background: var(--sidebar-bg); color: #fff; position: fixed; top: 0; left: 0; bottom: 0; overflow-y: auto; z-index: 100; }
.sidebar-header { padding: 24px 20px; border-bottom: 1px solid rgba(255,255,255,0.08); }
.sidebar-header .logo { width: 36px; height: 36px; background: var(--accent); border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 18px; font-weight: 700; margin-bottom: 12px; }
.sidebar-header h1 { font-size: 15px; font-weight: 700; line-height: 1.3; }
.sidebar-header p { font-size: 11px; color: rgba(255,255,255,0.5); margin-top: 4px; }
.sidebar-section { padding: 16px 0 8px; }
.sidebar-section-label { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; color: rgba(255,255,255,0.35); padding: 0 20px; margin-bottom: 4px; }
.nav-item { display: flex; align-items: center; gap: 12px; padding: 10px 20px; color: rgba(255,255,255,0.65); cursor: pointer; transition: all 0.15s; border-left: 3px solid transparent; font-size: 14px; font-weight: 500; }
.nav-item:hover { background: var(--sidebar-hover); color: #fff; }
.nav-item.active { background: rgba(255,54,33,0.12); color: #fff; border-left-color: var(--sidebar-active); }
.nav-item .nav-icon { width: 18px; height: 18px; flex-shrink: 0; }
.sidebar-footer { padding: 16px 20px; border-top: 1px solid rgba(255,255,255,0.08); font-size: 11px; color: rgba(255,255,255,0.4); margin-top: auto; }
.content { flex: 1; margin-left: 250px; padding: 32px; max-width: 1400px; }
.section-header { margin-bottom: 28px; }
.section-header h2 { font-size: 26px; font-weight: 700; letter-spacing: -0.5px; }
.section-header p { font-size: 14px; color: var(--text-secondary); margin-top: 4px; }
.card { background: var(--card-bg); border-radius: var(--radius); box-shadow: var(--shadow); padding: 24px; margin-bottom: 20px; }
.card-title { font-size: 12px; font-weight: 700; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 16px; display: flex; align-items: center; gap: 8px; }
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 16px; margin-bottom: 28px; }
.kpi-card { background: var(--card-bg); border-radius: var(--radius); box-shadow: var(--shadow); padding: 20px; }
.kpi-label { font-size: 11px; font-weight: 700; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.8px; }
.kpi-value { font-size: 28px; font-weight: 700; margin: 8px 0 4px; letter-spacing: -0.5px; }
.kpi-sub { font-size: 12px; color: var(--text-muted); }
.badge { display: inline-flex; align-items: center; padding: 3px 10px; border-radius: 20px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
.badge-success { background: #e8f5e9; color: #00a854; }
.badge-warning { background: #fff8e1; color: #b87400; }
.badge-error { background: #fdecea; color: #e53935; }
.badge-info { background: #e3f2fd; color: #0066ff; }
.badge-neutral { background: #f0f0f5; color: #6b6b76; }
.badge-bronze { background: #fff3e0; color: #b87400; }
.badge-silver { background: #f3e5f5; color: #7b1fa2; }
.badge-gold { background: #e8f5e9; color: #00a854; }
.data-table-wrap { overflow-x: auto; }
.data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.data-table th { text-align: left; padding: 10px 14px; font-size: 11px; font-weight: 700; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 2px solid var(--border); white-space: nowrap; }
.data-table td { padding: 12px 14px; border-bottom: 1px solid var(--border); }
.data-table tr:hover td { background: #f9f9fb; }
.data-table tr:last-child td { border-bottom: none; }
.table-name { font-family: "SF Mono", Monaco, "Cascadia Code", monospace; font-size: 12px; font-weight: 600; }
.source-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 16px; }
.source-card { background: var(--card-bg); border-radius: var(--radius); box-shadow: var(--shadow); padding: 20px; border-top: 3px solid var(--accent-blue); }
.source-card h3 { font-size: 16px; font-weight: 700; margin-bottom: 4px; }
.source-card .source-desc { font-size: 12px; color: var(--text-secondary); margin-bottom: 14px; }
.source-detail { display: flex; justify-content: space-between; padding: 6px 0; border-bottom: 1px solid var(--border); font-size: 13px; }
.source-detail:last-child { border-bottom: none; }
.source-detail-label { color: var(--text-secondary); font-weight: 500; }
.source-detail-value { font-weight: 600; text-align: right; }
.arch-flow { display: flex; align-items: stretch; gap: 0; margin-top: 8px; }
.arch-stage { flex: 1; padding: 20px; border-radius: var(--radius); text-align: center; }
.arch-stage h3 { font-size: 14px; font-weight: 700; margin-bottom: 6px; }
.arch-stage p { font-size: 12px; color: var(--text-secondary); }
.arch-stage .arch-count { font-size: 24px; font-weight: 700; margin: 4px 0; }
.arch-arrow { display: flex; align-items: center; padding: 0 4px; color: var(--text-muted); font-size: 20px; }
.arch-source { background: #e3f2fd; }
.arch-bronze { background: #fff3e0; }
.arch-silver { background: #f3e5f5; }
.arch-gold { background: #e8f5e9; }
.arch-dashboard { background: #fce4ec; }
.lineage-diagram { display: flex; gap: 4px; overflow-x: auto; padding-bottom: 12px; }
.lineage-column { min-width: 200px; flex: 1; }
.lineage-column-header { font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; text-align: center; padding: 8px; border-radius: 8px; margin-bottom: 12px; }
.lineage-column-header.source { background: #e3f2fd; color: #0066ff; }
.lineage-column-header.bronze { background: #fff3e0; color: #b87400; }
.lineage-column-header.silver { background: #f3e5f5; color: #7b1fa2; }
.lineage-column-header.gold { background: #e8f5e9; color: #00a854; }
.lineage-column-header.dashboard { background: #fce4ec; color: #ff3621; }
.lineage-node { padding: 10px 14px; border-radius: 8px; margin-bottom: 8px; font-size: 12px; font-weight: 500; background: #f9f9fb; border: 1px solid var(--border); }
.lineage-node.source { border-left: 3px solid #0066ff; }
.lineage-node.bronze { border-left: 3px solid #ff9f1c; }
.lineage-node.silver { border-left: 3px solid #9c27b0; }
.lineage-node.gold { border-left: 3px solid #00a854; }
.lineage-node.dashboard { border-left: 3px solid #ff3621; }
.lineage-node-name { font-family: "SF Mono", Monaco, "Cascadia Code", monospace; font-size: 11px; font-weight: 600; }
.lineage-arrow-col { display: flex; align-items: center; color: var(--text-muted); font-size: 20px; }
.dq-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 16px; }
.dq-card { background: var(--card-bg); border-radius: var(--radius); box-shadow: var(--shadow); padding: 20px; border-left: 3px solid var(--success); }
.dq-card h3 { font-size: 16px; font-weight: 700; margin-bottom: 4px; }
.dq-card .dq-table { font-family: "SF Mono", Monaco, "Cascadia Code", monospace; font-size: 11px; color: var(--text-secondary); margin-bottom: 12px; }
.dq-check-list { list-style: none; }
.dq-check-list li { padding: 6px 0; font-size: 13px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 8px; }
.dq-check-list li:last-child { border-bottom: none; }
.dq-check-icon { color: var(--error); font-size: 14px; }
.info-row { display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid var(--border); }
.info-row:last-child { border-bottom: none; }
.info-label { color: var(--text-secondary); font-weight: 500; font-size: 13px; }
.info-value { font-weight: 600; font-size: 13px; }
.tab-content { display: none; }
.tab-content.active { display: block; animation: fadeIn 0.2s ease; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }
.empty-state { text-align: center; padding: 48px 24px; color: var(--text-muted); }
.dash-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 16px; }
.dash-card { background: var(--card-bg); border-radius: var(--radius); box-shadow: var(--shadow); padding: 20px; border-top: 3px solid var(--accent); transition: transform 0.15s, box-shadow 0.15s; }
.dash-card:hover { transform: translateY(-2px); box-shadow: var(--shadow-lg); }
.dash-card h3 { font-size: 16px; font-weight: 700; margin-bottom: 6px; }
.dash-card p { font-size: 13px; color: var(--text-secondary); margin-bottom: 12px; }
.dash-card .dash-source { font-family: "SF Mono", Monaco, "Cascadia Code", monospace; font-size: 11px; color: var(--text-muted); }
</style>
</head>
<body>
<div class="app">
<aside class="sidebar">
<div class="sidebar-header">
<div class="logo">&#9889;</div>
<h1>Data Platform<br>Command Center</h1>
<p>Retail Data Platform &#183; Presales Demo</p>
</div>
<div class="sidebar-section">
<div class="sidebar-section-label">Monitoring</div>
<div class="nav-item active" data-tab="overview" onclick="switchTab('overview')"><svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>Overview</div>
<div class="nav-item" data-tab="sources" onclick="switchTab('sources')"><svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>Sources</div>
<div class="nav-item" data-tab="pipelines" onclick="switchTab('pipelines')"><svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="6" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><line x1="6" y1="9" x2="6" y2="15"/><path d="M16 6h6"/><path d="M16 18h6"/><path d="M16 12h6"/></svg>Pipelines</div>
<div class="nav-item" data-tab="tables" onclick="switchTab('tables')"><svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="12" y1="3" x2="12" y2="21"/></svg>Tables</div>
<div class="nav-item" data-tab="runs" onclick="switchTab('runs')"><svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>Pipeline Runs</div>
<div class="nav-item" data-tab="quality" onclick="switchTab('quality')"><svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>Data Quality</div>
<div class="nav-item" data-tab="dashboards" onclick="switchTab('dashboards')"><svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>Dashboards</div>
<div class="nav-item" data-tab="lineage" onclick="switchTab('lineage')"><svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="5" r="3"/><circle cx="5" cy="19" r="3"/><circle cx="19" cy="19" r="3"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="12" x2="7" y2="17"/><line x1="12" y1="12" x2="17" y2="17"/></svg>Lineage</div>
</div>
<div class="sidebar-footer">Pipeline: {{ pipeline_name }}<br>v1.0 &#183; Databricks Apps</div>
</aside>
<main class="content">
<!-- ═══ Overview ═══ -->
<section id="overview" class="tab-content active">
<div class="section-header"><h2>Platform Overview</h2><p>End-to-end visibility into the retail data platform</p></div>
<div class="kpi-grid">
<div class="kpi-card"><div class="kpi-label">Pipeline State</div><div class="kpi-value">{% if pipeline_info %}<span class="badge {% if pipeline_info.state in ['RUNNING','COMPLETED'] %}badge-success{% elif pipeline_info.state in ['IDLE','STARTING','STOPPING'] %}badge-warning{% elif pipeline_info.state in ['FAILED','STOPPED'] %}badge-error{% else %}badge-neutral{% endif %}">{{ pipeline_info.state }}</span>{% else %}<span class="badge badge-neutral">UNKNOWN</span>{% endif %}</div><div class="kpi-sub">{{ pipeline_name }}</div></div>
<div class="kpi-card"><div class="kpi-label">Data Sources</div><div class="kpi-value">{{ sources|length }}</div><div class="kpi-sub">Auto Loader (CSV)</div></div>
<div class="kpi-card"><div class="kpi-label">Total Tables</div><div class="kpi-value">{{ bronze_tables|length + silver_tables|length + quarantine_tables|length + gold_tables|length }}</div><div class="kpi-sub">Bronze + Silver + Gold</div></div>
<div class="kpi-card"><div class="kpi-label">Gold Aggregations</div><div class="kpi-value">{{ gold_tables|length }}</div><div class="kpi-sub">Materialized Views</div></div>
</div>
<div class="card"><div class="card-title">Platform Architecture</div><div class="arch-flow">
<div class="arch-stage arch-source"><h3>Sources</h3><div class="arch-count">{{ sources|length }}</div><p>Auto Loader CSV</p></div>
<div class="arch-arrow">&#8594;</div>
<div class="arch-stage arch-bronze"><h3>Bronze</h3><div class="arch-count">{{ bronze_tables|length }}</div><p>Streaming Tables</p></div>
<div class="arch-arrow">&#8594;</div>
<div class="arch-stage arch-silver"><h3>Silver</h3><div class="arch-count">{{ silver_tables|length + quarantine_tables|length }}</div><p>Auto CDC + Quarantine</p></div>
<div class="arch-arrow">&#8594;</div>
<div class="arch-stage arch-gold"><h3>Gold</h3><div class="arch-count">{{ gold_tables|length }}</div><p>Materialized Views</p></div>
<div class="arch-arrow">&#8594;</div>
<div class="arch-stage arch-dashboard"><h3>Dashboards</h3><div class="arch-count">1</div><p>BI &amp; Analytics</p></div>
</div></div>
<div class="card"><div class="card-title">Pipeline Health</div>
<div class="info-row"><span class="info-label">Pipeline Name</span><span class="info-value">{{ pipeline_name }}</span></div>
{% if pipeline_info %}
<div class="info-row"><span class="info-label">State</span><span class="info-value"><span class="badge {% if pipeline_info.state in ['RUNNING','COMPLETED'] %}badge-success{% elif pipeline_info.state in ['IDLE','STARTING','STOPPING'] %}badge-warning{% elif pipeline_info.state in ['FAILED','STOPPED'] %}badge-error{% else %}badge-neutral{% endif %}">{{ pipeline_info.state }}</span></span></div>
<div class="info-row"><span class="info-label">Pipeline Type</span><span class="info-value">{{ pipeline_info.pipeline_type }}</span></div>
<div class="info-row"><span class="info-label">Serverless</span><span class="info-value">{{ "Enabled" if pipeline_info.serverless else "Disabled" }}</span></div>
<div class="info-row"><span class="info-label">Photon</span><span class="info-value">{{ "Enabled" if pipeline_info.photon else "Disabled" }}</span></div>
<div class="info-row"><span class="info-label">Channel</span><span class="info-value">{{ pipeline_info.channel }}</span></div>
<div class="info-row"><span class="info-label">Storage</span><span class="info-value">{{ pipeline_info.catalog }}.{{ pipeline_info.schema }}</span></div>
<div class="info-row"><span class="info-label">Creator</span><span class="info-value">{{ pipeline_info.creator }}</span></div>
{% else %}
<div class="empty-state"><p>Live pipeline data unavailable. Static metadata shown.</p></div>
{% endif %}
</div>
</section>
<!-- ═══ Sources ═══ -->
<section id="sources" class="tab-content">
<div class="section-header"><h2>Data Sources</h2><p>{{ sources|length }} sources ingested via Auto Loader from Unity Catalog Volume</p></div>
<div class="card"><div class="card-title">Volume Location</div>
<div class="info-row"><span class="info-label">UC Volume Path</span><span class="info-value" style="font-family:'SF Mono',Monaco,monospace;font-size:12px;">/Volumes/retail/raw/data</span></div>
<div class="info-row"><span class="info-label">Format</span><span class="info-value">CSV (with header)</span></div>
<div class="info-row"><span class="info-label">Schema Evolution</span><span class="info-value">addNewColumns</span></div>
<div class="info-row"><span class="info-label">Ingestion Method</span><span class="info-value">Auto Loader (cloudFiles)</span></div>
</div>
<div class="source-grid">
{% for src in sources %}
<div class="source-card"><h3>{{ src.name }}</h3><p class="source-desc">{{ src.description }}</p>
<div class="source-detail"><span class="source-detail-label">Format</span><span class="source-detail-value">{{ src.format }}</span></div>
<div class="source-detail"><span class="source-detail-label">Cadence</span><span class="source-detail-value">{{ src.cadence }}</span></div>
<div class="source-detail"><span class="source-detail-label">SLA</span><span class="source-detail-value">{{ src.sla }}</span></div>
<div class="source-detail"><span class="source-detail-label">Schema Evolution</span><span class="source-detail-value">{{ src.schema_evo }}</span></div>
<div class="source-detail"><span class="source-detail-label">Path</span><span class="source-detail-value" style="font-family:'SF Mono',Monaco,monospace;font-size:10px;">{{ src.path }}</span></div>
<div class="source-detail"><span class="source-detail-label">Columns</span><span class="source-detail-value">{{ src.columns|length }}</span></div>
</div>
{% endfor %}
</div>
</section>
<!-- ═══ Pipelines ═══ -->
<section id="pipelines" class="tab-content">
<div class="section-header"><h2>Pipelines</h2><p>Spark Declarative Pipeline (SDP) configuration and health</p></div>
<div class="card"><div class="card-title">Pipeline Details</div>
<div class="info-row"><span class="info-label">Pipeline Name</span><span class="info-value">{{ pipeline_name }}</span></div>
<div class="info-row"><span class="info-label">Pipeline ID</span><span class="info-value" style="font-family:'SF Mono',Monaco,monospace;font-size:11px;">{{ pipeline_id }}</span></div>
{% if pipeline_info %}
<div class="info-row"><span class="info-label">State</span><span class="info-value"><span class="badge {% if pipeline_info.state in ['RUNNING','COMPLETED'] %}badge-success{% elif pipeline_info.state in ['IDLE','STARTING','STOPPING'] %}badge-warning{% elif pipeline_info.state in ['FAILED','STOPPED'] %}badge-error{% else %}badge-neutral{% endif %}">{{ pipeline_info.state }}</span></span></div>
<div class="info-row"><span class="info-label">Pipeline Type</span><span class="info-value">{{ pipeline_info.pipeline_type }}</span></div>
<div class="info-row"><span class="info-label">Serverless</span><span class="info-value">{{ "Enabled" if pipeline_info.serverless else "Disabled" }}</span></div>
<div class="info-row"><span class="info-label">Photon</span><span class="info-value">{{ "Enabled" if pipeline_info.photon else "Disabled" }}</span></div>
<div class="info-row"><span class="info-label">Channel</span><span class="info-value">{{ pipeline_info.channel }}</span></div>
<div class="info-row"><span class="info-label">Storage Catalog</span><span class="info-value">{{ pipeline_info.catalog }}</span></div>
<div class="info-row"><span class="info-label">Storage Schema</span><span class="info-value">{{ pipeline_info.schema }}</span></div>
<div class="info-row"><span class="info-label">Creator</span><span class="info-value">{{ pipeline_info.creator }}</span></div>
{% endif %}
</div>
<div class="card"><div class="card-title">Pipeline Libraries</div>
<div class="info-row"><span class="info-label">Type</span><span class="info-value">Glob</span></div>
<div class="info-row"><span class="info-label">Path Pattern</span><span class="info-value" style="font-family:'SF Mono',Monaco,monospace;font-size:11px;">/Workspace/Users/rahuljhaver.1990@gmail.com/Pipeline-example1/transformations/**</span></div>
</div>
<div class="card"><div class="card-title">Pipeline Processing Summary</div>
<div class="info-row"><span class="info-label">Bronze Layer</span><span class="info-value">4 Auto Loader streaming tables with schema evolution</span></div>
<div class="info-row"><span class="info-label">Silver Layer</span><span class="info-value">4 Auto CDC flows (SCD Type 1) + 4 quarantine tables</span></div>
<div class="info-row"><span class="info-label">Gold Layer</span><span class="info-value">4 incremental materialized views</span></div>
<div class="info-row"><span class="info-label">CDC Mode</span><span class="info-value">Auto CDC with SCD Type 1</span></div>
<div class="info-row"><span class="info-label">DQ Validation</span><span class="info-value">Inline validation with quarantine routing</span></div>
</div>
</section>
<!-- ═══ Tables ═══ -->
<section id="tables" class="tab-content">
<div class="section-header"><h2>Tables</h2><p>{{ bronze_tables|length + silver_tables|length + quarantine_tables|length + gold_tables|length }} tables across the medallion architecture</p></div>
<div class="card"><div class="card-title"><span class="badge badge-bronze">Bronze</span> {{ bronze_tables|length }} Streaming Tables</div>
<div class="data-table-wrap"><table class="data-table"><thead><tr><th>Table Name</th><th>Source</th><th>Type</th><th>Trigger</th><th>Primary Key</th><th>DQ Validation</th></tr></thead><tbody>
{% for t in bronze_tables %}
<tr><td class="table-name">{{ t.name }}</td><td>{{ t.source }}</td><td>{{ t.type }}</td><td>{{ t.trigger }}</td><td>{{ t.key }}</td><td>{% if t.dq %}<span class="badge badge-success">Enabled</span>{% else %}<span class="badge badge-neutral">N/A</span>{% endif %}</td></tr>
{% endfor %}
</tbody></table></div></div>
<div class="card"><div class="card-title"><span class="badge badge-silver">Silver</span> {{ silver_tables|length }} Auto CDC + {{ quarantine_tables|length }} Quarantine</div>
<div class="data-table-wrap"><table class="data-table"><thead><tr><th>Table Name</th><th>Source</th><th>Type</th><th>Primary Key</th><th>SCD Type</th><th>Quarantine Table</th></tr></thead><tbody>
{% for t in silver_tables %}
<tr><td class="table-name">{{ t.name }}</td><td>{{ t.source }}</td><td>{{ t.type }}</td><td>{{ t.key }}</td><td>SCD Type 1</td><td>{{ t.quarantine }}</td></tr>
{% endfor %}
{% for t in quarantine_tables %}
<tr><td class="table-name">{{ t.name }}</td><td>{{ t.source }}</td><td>Quarantine</td><td>&mdash;</td><td>&mdash;</td><td>&mdash;</td></tr>
{% endfor %}
</tbody></table></div></div>
<div class="card"><div class="card-title"><span class="badge badge-gold">Gold</span> {{ gold_tables|length }} Materialized Views</div>
<div class="data-table-wrap"><table class="data-table"><thead><tr><th>Table Name</th><th>Source</th><th>Type</th><th>Aggregation</th><th>Metrics</th></tr></thead><tbody>
{% for t in gold_tables %}
<tr><td class="table-name">{{ t.name }}</td><td>{{ t.source }}</td><td>{{ t.type }}</td><td>{{ t.agg }}</td><td>{{ t.metrics }}</td></tr>
{% endfor %}
</tbody></table></div></div>
</section>
<!-- ═══ Pipeline Runs ═══ -->
<section id="runs" class="tab-content">
<div class="section-header"><h2>Pipeline Runs</h2><p>Update history for {{ pipeline_name }}</p></div>
<div class="card"><div class="card-title">Run History</div>
{% if pipeline_updates %}
<div class="data-table-wrap"><table class="data-table"><thead><tr><th>Update ID</th><th>State</th><th>Created</th><th>Type</th></tr></thead><tbody>
{% for u in pipeline_updates %}
<tr><td style="font-family:'SF Mono',Monaco,monospace;font-size:11px;">{{ u.update_id }}</td><td><span class="badge {% if u.state in ['COMPLETED','SUCCEEDED'] %}badge-success{% elif u.state in ['RUNNING','ACTIVE'] %}badge-info{% elif u.state in ['FAILED','ERROR'] %}badge-error{% elif u.state in ['PENDING','WAITING'] %}badge-warning{% else %}badge-neutral{% endif %}">{{ u.state }}</span></td><td>{{ u.creation_time }}</td><td>{{ u.update_type }}</td></tr>
{% endfor %}
</tbody></table></div>
{% else %}
<div class="empty-state"><p>No run history available.</p></div>
{% endif %}
</div>
</section>
<!-- ═══ Data Quality ═══ -->
<section id="quality" class="tab-content">
<div class="section-header"><h2>Data Quality</h2><p>Inline validation checks and quarantine routing</p></div>
<div class="card"><div class="card-title">DQ Validation Approach</div>
<div class="info-row"><span class="info-label">Method</span><span class="info-value">Inline column validation with when() conditions</span></div>
<div class="info-row"><span class="info-label">Validation Fields</span><span class="info-value">_dq_is_valid (boolean), _dq_failure_reason (string)</span></div>
<div class="info-row"><span class="info-label">Quarantine Strategy</span><span class="info-value">Failed records routed to quarantine tables</span></div>
<div class="info-row"><span class="info-label">Valid Record Flow</span><span class="info-value">Passed records proceed to Auto CDC (SCD Type 1)</span></div>
</div>
<div class="dq-grid">
{% for dq in dq_checks %}
<div class="dq-card"><h3>{{ dq.entity }}</h3><div class="dq-table">{{ dq.table }}</div>
<ul class="dq-check-list">
{% for check in dq.checks %}
<li><span class="dq-check-icon">&#9888;</span> {{ check }}</li>
{% endfor %}
</ul>
<div style="margin-top:12px;padding-top:12px;border-top:1px solid var(--border);"><span style="font-size:12px;color:var(--text-secondary);">Quarantine: </span><span style="font-family:'SF Mono',Monaco,monospace;font-size:11px;font-weight:600;">{{ dq.quarantine }}</span></div>
</div>
{% endfor %}
</div>
</section>
<!-- ═══ Dashboards ═══ -->
<section id="dashboards" class="tab-content">
<div class="section-header"><h2>Dashboards</h2><p>Analytics powered by Gold layer materialized views</p></div>
<div class="dash-grid">
<div class="dash-card"><h3>Sales Analytics Dashboard</h3><p>Comprehensive sales analytics with daily trends, customer insights, store performance, and product breakdowns.</p><div class="dash-source">retail.gold.daily_sales &#183; sales_by_customer &#183; sales_by_store &#183; sales_by_product</div></div>
</div>
<div class="card" style="margin-top:20px;"><div class="card-title">Available Gold Tables for Dashboards</div>
<div class="data-table-wrap"><table class="data-table"><thead><tr><th>Table</th><th>Aggregation</th><th>Metrics</th></tr></thead><tbody>
{% for t in gold_tables %}
<tr><td class="table-name">{{ t.name }}</td><td>{{ t.agg }}</td><td>{{ t.metrics }}</td></tr>
{% endfor %}
</tbody></table></div></div>
</section>
<!-- ═══ Lineage ═══ -->
<section id="lineage" class="tab-content">
<div class="section-header"><h2>Data Lineage</h2><p>End-to-end data flow from sources to dashboards</p></div>
<div class="card"><div class="card-title">Medallion Architecture Lineage</div>
<div class="lineage-diagram">
<div class="lineage-column"><div class="lineage-column-header source">Sources</div>
{% for src in sources %}
<div class="lineage-node source"><div class="lineage-node-name">{{ src.name }} CSV</div><div style="font-size:10px;color:var(--text-muted);">{{ src.cadence }}</div></div>
{% endfor %}
</div>
<div class="lineage-arrow-col">&#8594;</div>
<div class="lineage-column"><div class="lineage-column-header bronze">Bronze</div>
{% for t in bronze_tables %}
<div class="lineage-node bronze"><div class="lineage-node-name">{{ t.name }}</div></div>
{% endfor %}
</div>
<div class="lineage-arrow-col">&#8594;</div>
<div class="lineage-column"><div class="lineage-column-header silver">Silver</div>
{% for t in silver_tables %}
<div class="lineage-node silver"><div class="lineage-node-name">{{ t.name }}</div></div>
{% endfor %}
{% for t in quarantine_tables %}
<div class="lineage-node silver" style="opacity:0.6;"><div class="lineage-node-name">{{ t.name }}</div><div style="font-size:10px;color:var(--text-muted);">quarantine</div></div>
{% endfor %}
</div>
<div class="lineage-arrow-col">&#8594;</div>
<div class="lineage-column"><div class="lineage-column-header gold">Gold</div>
{% for t in gold_tables %}
<div class="lineage-node gold"><div class="lineage-node-name">{{ t.name }}</div></div>
{% endfor %}
</div>
<div class="lineage-arrow-col">&#8594;</div>
<div class="lineage-column"><div class="lineage-column-header dashboard">Dashboards</div>
<div class="lineage-node dashboard"><div class="lineage-node-name">Sales Analytics Dashboard</div></div>
</div>
</div></div>
<div class="card"><div class="card-title">Lineage Summary</div>
<div class="info-row"><span class="info-label">Data Flow</span><span class="info-value">Sources &#8594; Bronze &#8594; Silver &#8594; Gold &#8594; Dashboards</span></div>
<div class="info-row"><span class="info-label">Ingestion</span><span class="info-value">Auto Loader (cloudFiles) with schema evolution</span></div>
<div class="info-row"><span class="info-label">Bronze &#8594; Silver</span><span class="info-value">DQ validation &#8594; quarantine (failed) / Auto CDC SCD1 (passed)</span></div>
<div class="info-row"><span class="info-label">Silver &#8594; Gold</span><span class="info-value">Incremental materialized views (aggregations)</span></div>
<div class="info-row"><span class="info-label">Gold &#8594; Dashboards</span><span class="info-value">BI consumption via SQL queries</span></div>
</div>
</section>
</main>
</div>
<script>
function switchTab(tabName) {
    document.querySelectorAll('.tab-content').forEach(function(s) { s.classList.remove('active'); });
    document.querySelectorAll('.nav-item').forEach(function(n) { n.classList.remove('active'); });
    document.getElementById(tabName).classList.add('active');
    document.querySelector('[data-tab="' + tabName + '"]').classList.add('active');
}
</script>
</body>
</html>
'''

# ─── Routes ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    pipeline_info = get_pipeline_info()
    pipeline_updates = get_pipeline_updates()
    return render_template_string(
        TEMPLATE,
        pipeline_info=pipeline_info,
        pipeline_updates=pipeline_updates,
        sources=SOURCES,
        bronze_tables=BRONZE_TABLES,
        silver_tables=SILVER_TABLES,
        quarantine_tables=QUARANTINE_TABLES,
        gold_tables=GOLD_TABLES,
        dq_checks=DQ_CHECKS,
        pipeline_name=PIPELINE_NAME,
        pipeline_id=PIPELINE_ID,
    )

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "pipeline": PIPELINE_NAME, "pipeline_id": PIPELINE_ID})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))