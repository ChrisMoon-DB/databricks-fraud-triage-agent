# Agentic Fraud Triage Platform

A full-stack fraud detection and investigation platform built on Databricks, designed for banking and financial services.

## Architecture

```
CSV Files (Unity Catalog Volume)
    │
    ▼
┌──────────────────────────────┐
│  DLT Pipeline (Lakeflow)     │
│  Bronze → Silver → Gold      │
│  5 fraud detection rules      │
└──────────┬───────────────────┘
           │
   ┌───────┼────────┐
   ▼       ▼        ▼
┌──────┐ ┌──────┐ ┌──────────┐
│ PII  │ │  AI  │ │  Genie   │
│Mask  │ │Agent │ │  Space   │
└──────┘ └──┬───┘ └──────────┘
            │
            ▼
     ┌─────────────┐
     │  Lakebase    │
     │  (Postgres)  │
     └──────┬──────┘
            │
            ▼
     ┌─────────────┐
     │  Sentinel    │
     │  App (Flask) │
     └─────────────┘
```

## Components

### Notebooks

| # | Notebook | Purpose |
|---|----------|---------|
| 00 | `00_environment_setup` | Creates catalog, schemas, volume — run this first |
| 01 | `01_pii_masking_setup` | Unity Catalog masking functions + secure views for PII protection |
| 02 | `02_dlt_fraud_pipeline` | Lakeflow DLT pipeline: ingest CSVs → enrich → flag fraud |
| 03 | `03_risk_scoring_agent` | AI reasoning agent using Foundation Model API for explainable risk scores |
| 04 | `04_lakebase_triage_store` | Lakebase Postgres provisioning + upsert service |
| 05 | `05_genie_space_queries` | Certified SQL queries for Genie Space (banking KPIs) |

### Databricks App — Sentinel Fraud Defense Platform

A two-tab Flask application:

- **Fraud Queue** — Real-time triage dashboard backed by Lakebase. Analysts review, release, block, or escalate flagged transactions.
- **Investigator** — Conversational fraud investigation interface backed by Databricks Genie. Ask natural language questions about transactions, anomalies, and fraud KPIs.

## Fraud Detection Rules

| Rule | Condition | Risk Points |
|------|-----------|-------------|
| High-Value Wire | Wire transfer > $10,000 | +25 |
| Wire + MFA Change | Wire > $10K AND MFA recently changed | +30 |
| Impossible Travel | Login > 500 miles in < 10 minutes | +25 |
| Bot Typing | Typing cadence < 80ms | +10 |
| Crypto High-Value | Amount > $5K to crypto merchant | +10 |

### Triage Classification

| Score | Status | Action |
|-------|--------|--------|
| ≥ 50 | `RED_BLOCK` | Auto-blocked |
| 20–49 | `YELLOW_REVIEW` | Held for analyst review |
| < 20 | `GREEN_ALLOW` | Approved |

### Sample Data

The `data/` directory contains mock banking datasets:

| File | Records | Description |
|------|---------|-------------|
| `transactions.csv` | 10,000 | Transaction data with amounts, merchant info, geolocation, fraud labels |
| `login_logs.csv` | 8,000 | Login sessions with IPs, MFA changes, typing cadence, impossible travel flags |
| `users.csv` | 500 | User profiles with names, emails, card numbers |

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Warehouse
- Lakebase enabled on the workspace

## Setup

### 0. Environment Setup
Run `notebooks/00_environment_setup.py` to create the catalog, schemas, and volume. Then upload the sample data:
```bash
databricks fs cp data/transactions.csv dbfs:/Volumes/cmoon_financial_security/fraud_raw/source_files/transactions.csv
databricks fs cp data/login_logs.csv dbfs:/Volumes/cmoon_financial_security/fraud_raw/source_files/login_logs.csv
databricks fs cp data/users.csv dbfs:/Volumes/cmoon_financial_security/fraud_raw/source_files/users.csv
```

### 1. Data Pipeline
Import `notebooks/02_dlt_fraud_pipeline.py` and create a DLT pipeline pointing to it with:
- Catalog: `cmoon_financial_security`
- Target schema: `fraud_silver`
- Serverless: enabled

### 2. PII Masking
Run `notebooks/01_pii_masking_setup.py` to create masking functions and secure views.

### 3. Risk Scoring Agent
Run `notebooks/03_risk_scoring_agent.py` to generate AI-powered risk explanations.

### 4. Lakebase Triage Store
Run `notebooks/04_lakebase_triage_store.py` to provision Lakebase and upsert flagged transactions.

### 5. Genie Space
Create a Genie Space in the workspace UI using queries from `notebooks/05_genie_space_queries.sql`.

### 6. Databricks App
Deploy the app from the `app/` directory:
```bash
databricks apps create --json '{"name": "fraud-queue", "description": "Sentinel Fraud Defense Platform"}'
databricks workspace mkdirs /Workspace/Users/<you>/fraud_queue_app/static
# Upload app files to workspace, then:
databricks apps deploy fraud-queue --source-code-path /Workspace/Users/<you>/fraud_queue_app
```

## Customer Requirements Addressed

- **Explainability** — Every blocked transaction includes a human-readable justification (GDPR/CCPA compliant)
- **Low-Latency** — Lakebase provides sub-second lookups for block/allow decisions
- **PII Protection** — Unity Catalog masking functions ensure analysts see only necessary data
