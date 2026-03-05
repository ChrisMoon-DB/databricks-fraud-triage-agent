# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Environment Setup
# MAGIC Creates the Unity Catalog, schemas, volume, and uploads sample data.
# MAGIC Run this notebook first to set up the environment before running the other notebooks.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Upload the CSV files from the `data/` directory in this repo to a location accessible by this notebook
# MAGIC   (e.g., a workspace volume or DBFS), OR use the file upload widget below.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS cmoon_financial_security
# MAGIC COMMENT 'Financial Security catalog for fraud detection and triage';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG cmoon_financial_security;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fraud_raw
# MAGIC COMMENT 'Raw ingestion layer - source files and bronze tables';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fraud_silver
# MAGIC COMMENT 'Silver layer - enriched and flagged tables from DLT pipeline';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fraud_serving
# MAGIC COMMENT 'Serving layer - masked views, AI assessments, and app-facing data';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create Volume for Source Files

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS fraud_raw.source_files
# MAGIC COMMENT 'Volume for raw CSV source files (transactions, login logs, users)';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Upload Sample Data to Volume
# MAGIC
# MAGIC Upload the CSV files from the `data/` directory in this repo.
# MAGIC You can either:
# MAGIC 1. Use the Databricks UI to upload files to the volume
# MAGIC 2. Use the Databricks CLI:
# MAGIC    ```
# MAGIC    databricks fs cp data/transactions.csv dbfs:/Volumes/cmoon_financial_security/fraud_raw/source_files/transactions.csv
# MAGIC    databricks fs cp data/login_logs.csv dbfs:/Volumes/cmoon_financial_security/fraud_raw/source_files/login_logs.csv
# MAGIC    databricks fs cp data/users.csv dbfs:/Volumes/cmoon_financial_security/fraud_raw/source_files/users.csv
# MAGIC    ```
# MAGIC 3. Or run the cell below if the files are already in the repo workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Verify Volume Contents

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/cmoon_financial_security/fraud_raw/source_files/';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Preview Source Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview transactions
# MAGIC SELECT * FROM csv.`/Volumes/cmoon_financial_security/fraud_raw/source_files/transactions.csv`
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview login logs
# MAGIC SELECT * FROM csv.`/Volumes/cmoon_financial_security/fraud_raw/source_files/login_logs.csv`
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview users
# MAGIC SELECT * FROM csv.`/Volumes/cmoon_financial_security/fraud_raw/source_files/users.csv`
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Verify Setup Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Catalog' AS object_type, catalog_name AS name FROM system.information_schema.catalogs WHERE catalog_name = 'cmoon_financial_security'
# MAGIC UNION ALL
# MAGIC SELECT 'Schema', schema_name FROM cmoon_financial_security.information_schema.schemata WHERE schema_name IN ('fraud_raw', 'fraud_silver', 'fraud_serving')
# MAGIC ORDER BY object_type, name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment is ready!
# MAGIC
# MAGIC Next steps:
# MAGIC 1. **Upload CSV files** to the volume (if not done already)
# MAGIC 2. Run **02_dlt_fraud_pipeline** — Create and start the DLT pipeline
# MAGIC 3. Run **01_pii_masking_setup** — Apply PII masking views
# MAGIC 4. Run **03_risk_scoring_agent** — Generate AI risk explanations
# MAGIC 5. Run **04_lakebase_triage_store** — Set up Lakebase and upsert flagged transactions
# MAGIC 6. Create a **Genie Space** using queries from **05_genie_space_queries**
# MAGIC 7. Deploy the **Databricks App** from the `app/` directory
