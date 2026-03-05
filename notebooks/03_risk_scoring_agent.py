# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Fraud Risk Reasoning Agent
# MAGIC Uses Foundation Model API to analyze transaction metadata and provide
# MAGIC plain-English explanations for risk scores (GDPR/CCPA compliant explainability).

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from pyspark.sql.functions import col, udf, struct, to_json, lit
from pyspark.sql.types import StringType, StructType, StructField, FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the Reasoning Agent
# MAGIC The agent analyzes transaction metadata and produces a structured risk assessment.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def analyze_transaction_risk(transaction_json: str) -> str:
    """
    Calls Foundation Model API to generate a human-readable risk explanation.
    """
    prompt = f"""You are a senior bank fraud analyst AI. Analyze the following transaction metadata and provide:
1. A risk assessment (LOW / MEDIUM / HIGH / CRITICAL)
2. A confidence score (0.0 to 1.0)
3. A plain-English explanation suitable for regulatory compliance (GDPR/CCPA)
4. Recommended action (ALLOW / REVIEW / BLOCK)

Transaction data:
{transaction_json}

Respond in this exact JSON format:
{{"risk_level": "...", "confidence": 0.0, "explanation": "...", "recommended_action": "...", "regulatory_justification": "..."}}
"""
    response = w.serving_endpoints.query(
        name="databricks-claude-sonnet-4",
        messages=[
            {"role": "system", "content": "You are a fraud detection reasoning engine for a banking institution. Your explanations must be clear, factual, and compliant with financial regulations."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=500,
        temperature=0.1
    )
    return response.choices[0].message.content

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process Yellow-Flagged Transactions
# MAGIC Pull transactions that need AI review and generate explanations.

# COMMAND ----------

CATALOG = "cmoon_financial_security"

# Get yellow-flagged transactions (most need human review)
yellow_flagged = spark.sql(f"""
    SELECT transaction_id, user_id, transaction_timestamp, amount, currency,
           transaction_type, merchant_category, ip_address, risk_score,
           triage_status, risk_explanation,
           flag_high_value_wire, flag_wire_after_mfa_change,
           flag_impossible_travel, flag_bot_typing, flag_crypto_high_value,
           mfa_change_flag, typing_cadence_ms, velocity_mph
    FROM {CATALOG}.fraud_silver.transactions_flagged
    WHERE triage_status IN ('YELLOW_REVIEW', 'RED_BLOCK')
    ORDER BY risk_score DESC
    LIMIT 50
""")

display(yellow_flagged)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run AI Agent on Flagged Transactions

# COMMAND ----------

# Process a sample batch with the reasoning agent
flagged_rows = yellow_flagged.limit(10).collect()

results = []
for row in flagged_rows:
    txn_data = {
        "transaction_id": row.transaction_id,
        "amount": float(row.amount),
        "currency": row.currency,
        "type": row.transaction_type,
        "merchant_category": row.merchant_category,
        "risk_score": int(row.risk_score),
        "rule_flags": {
            "high_value_wire": row.flag_high_value_wire,
            "wire_after_mfa_change": row.flag_wire_after_mfa_change,
            "impossible_travel": row.flag_impossible_travel,
            "bot_typing": row.flag_bot_typing,
            "crypto_high_value": row.flag_crypto_high_value,
        },
        "mfa_changed": bool(row.mfa_change_flag),
        "typing_cadence_ms": row.typing_cadence_ms,
        "velocity_mph": row.velocity_mph,
    }

    try:
        ai_analysis = analyze_transaction_risk(json.dumps(txn_data, default=str))
        analysis_parsed = json.loads(ai_analysis)
    except Exception as e:
        analysis_parsed = {
            "risk_level": "UNKNOWN",
            "confidence": 0.0,
            "explanation": f"Analysis failed: {str(e)}",
            "recommended_action": "REVIEW",
            "regulatory_justification": "Manual review required due to analysis failure"
        }

    results.append({
        "transaction_id": row.transaction_id,
        "rule_risk_score": row.risk_score,
        "ai_risk_level": analysis_parsed.get("risk_level"),
        "ai_confidence": analysis_parsed.get("confidence"),
        "ai_explanation": analysis_parsed.get("explanation"),
        "ai_recommended_action": analysis_parsed.get("recommended_action"),
        "regulatory_justification": analysis_parsed.get("regulatory_justification"),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save AI-Enhanced Risk Assessments

# COMMAND ----------

results_df = spark.createDataFrame(results)
display(results_df)

# COMMAND ----------

# Write AI assessments to the serving schema
results_df.write.mode("overwrite").saveAsTable(
    f"{CATALOG}.fraud_serving.ai_risk_assessments"
)
print(f"Saved {len(results)} AI risk assessments to {CATALOG}.fraud_serving.ai_risk_assessments")