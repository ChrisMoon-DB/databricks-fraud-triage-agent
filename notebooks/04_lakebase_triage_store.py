# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Lakebase: Real-Time Fraud Triage Store
# MAGIC Sets up a Lakebase (Serverless Postgres) instance as the operational triage store.
# MAGIC Stores active session risks for sub-second, high-concurrency access by the core banking engine.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create Lakebase Database Instance
# MAGIC Creates a serverless Postgres instance. Skip if it already exists.

# COMMAND ----------

INSTANCE_NAME = "fraud-triage-store"

try:
    db = w.database.create_database_instance_and_wait(
        DatabaseInstance(name=INSTANCE_NAME, capacity="CU_1")
    )
    print(f"Lakebase instance created: {db.name}, state: {db.state}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Instance '{INSTANCE_NAME}' already exists, fetching details...")
        db = w.database.get_database_instance(INSTANCE_NAME)
        print(f"Instance: {db.name}, state: {db.state}")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Get Lakebase Connection Details

# COMMAND ----------

db = w.database.get_database_instance(INSTANCE_NAME)
print(f"Instance: {db.name}")
print(f"State:    {db.state}")
print(f"RW DNS:   {db.read_write_dns}")
print(f"RO DNS:   {db.read_only_dns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Connect and Create Triage Schema

# COMMAND ----------

import psycopg2

DB_NAME = "fraud_triage"

# Generate short-lived credential for Lakebase
cred = w.database.generate_database_credential(instance_names=[INSTANCE_NAME])
current_user = w.current_user.me().user_name

# First connect to default 'postgres' db to create our database
conn = psycopg2.connect(
    host=db.read_write_dns,
    port=5432,
    dbname="postgres",
    user=current_user,
    password=cred.token,
    sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()

# Create the fraud_triage database if it doesn't exist
cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
if not cur.fetchone():
    cur.execute(f"CREATE DATABASE {DB_NAME}")
    print(f"Database '{DB_NAME}' created!")
else:
    print(f"Database '{DB_NAME}' already exists.")

cur.close()
conn.close()

# Reconnect to the new database
cred = w.database.generate_database_credential(instance_names=[INSTANCE_NAME])
conn = psycopg2.connect(
    host=db.read_write_dns,
    port=5432,
    dbname=DB_NAME,
    user=current_user,
    password=cred.token,
    sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()
print(f"Connected to Lakebase database '{DB_NAME}'!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Create the Real-Time Triage Table

# COMMAND ----------

cur.execute("""
    CREATE TABLE IF NOT EXISTS real_time_fraud_triage (
        transaction_id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(20) NOT NULL,
        amount DECIMAL(15, 2) NOT NULL,
        currency VARCHAR(10) DEFAULT 'USD',
        transaction_type VARCHAR(30),
        merchant_category VARCHAR(30),
        risk_score INTEGER NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
        triage_status VARCHAR(20) NOT NULL DEFAULT 'YELLOW_REVIEW',
        automated_action VARCHAR(20) NOT NULL DEFAULT 'HOLD',
        risk_explanation TEXT,
        ai_explanation TEXT,
        regulatory_justification TEXT,
        analyst_decision VARCHAR(20),
        analyst_notes TEXT,
        flagged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        reviewed_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT valid_triage CHECK (triage_status IN ('RED_BLOCK', 'YELLOW_REVIEW', 'GREEN_ALLOW')),
        CONSTRAINT valid_action CHECK (automated_action IN ('BLOCK', 'HOLD', 'ALLOW')),
        CONSTRAINT valid_decision CHECK (analyst_decision IS NULL OR analyst_decision IN ('APPROVED', 'BLOCKED', 'ESCALATED'))
    );
""")

cur.execute("CREATE INDEX IF NOT EXISTS idx_triage_status ON real_time_fraud_triage(triage_status);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_triage_user ON real_time_fraud_triage(user_id);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_triage_risk ON real_time_fraud_triage(risk_score DESC);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_triage_flagged ON real_time_fraud_triage(flagged_at DESC);")

print("Lakebase triage table and indexes created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Upsert Flagged Transactions from Silver Layer into Lakebase

# COMMAND ----------

CATALOG = "cmoon_financial_security"

flagged_df = spark.sql(f"""
    SELECT transaction_id, user_id, amount, currency,
           transaction_type, merchant_category,
           risk_score, triage_status, risk_explanation
    FROM {CATALOG}.fraud_silver.transactions_flagged
    WHERE triage_status IN ('YELLOW_REVIEW', 'RED_BLOCK')
    ORDER BY risk_score DESC
""")

flagged_rows = flagged_df.collect()
print(f"Found {len(flagged_rows)} flagged transactions to upsert")

# COMMAND ----------

upsert_sql = """
    INSERT INTO real_time_fraud_triage
        (transaction_id, user_id, amount, currency, transaction_type,
         merchant_category, risk_score, triage_status, automated_action,
         risk_explanation)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id)
    DO UPDATE SET
        risk_score = EXCLUDED.risk_score,
        triage_status = EXCLUDED.triage_status,
        automated_action = EXCLUDED.automated_action,
        risk_explanation = EXCLUDED.risk_explanation,
        updated_at = CURRENT_TIMESTAMP;
"""

count = 0
for row in flagged_rows:
    automated_action = "BLOCK" if row.triage_status == "RED_BLOCK" else "HOLD"
    cur.execute(upsert_sql, (
        row.transaction_id, row.user_id, float(row.amount), row.currency,
        row.transaction_type, row.merchant_category, int(row.risk_score),
        row.triage_status, automated_action, row.risk_explanation
    ))
    count += 1

print(f"Upserted {count} records into Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Verify Data

# COMMAND ----------

cur.execute("""
    SELECT triage_status, COUNT(*) as cnt, ROUND(AVG(risk_score)::numeric, 1) as avg_risk
    FROM real_time_fraud_triage
    GROUP BY triage_status
    ORDER BY avg_risk DESC;
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} transactions, avg risk: {row[2]}")

# COMMAND ----------

cur.close()
conn.close()
print("Lakebase connection closed.")