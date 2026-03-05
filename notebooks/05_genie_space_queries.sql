-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 - Genie Space: Certified SQL Queries for Banking KPIs
-- MAGIC Curated queries for investigators and analysts.
-- MAGIC These can be registered as "Certified SQL" in a Databricks Genie Space.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### KPI 1: False Positive Ratio (FPR)
-- MAGIC Measures the ratio of legitimate transactions flagged as fraudulent.

-- COMMAND ----------

-- False Positive Ratio
SELECT
    triage_status,
    COUNT(*) AS total_flagged,
    SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) AS false_positives,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS true_positives,
    ROUND(
        SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2
    ) AS false_positive_rate_pct
FROM cmoon_financial_security.fraud_silver.transactions_flagged
WHERE triage_status != 'GREEN_ALLOW'
GROUP BY triage_status
ORDER BY triage_status

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### KPI 2: Account Takeover Rate
-- MAGIC Detects potential account takeovers based on MFA changes + high-value transactions.

-- COMMAND ----------

-- Account Takeover Rate
SELECT
    DATE(transaction_timestamp) AS txn_date,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN flag_wire_after_mfa_change = true THEN 1 ELSE 0 END) AS potential_ato_count,
    ROUND(
        SUM(CASE WHEN flag_wire_after_mfa_change = true THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 4
    ) AS ato_rate_pct,
    SUM(CASE WHEN flag_wire_after_mfa_change = true THEN amount ELSE 0 END) AS ato_exposure_usd
FROM cmoon_financial_security.fraud_silver.transactions_flagged
GROUP BY DATE(transaction_timestamp)
ORDER BY txn_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### KPI 3: Wire Transfers Over $10K with Recent MFA Changes
-- MAGIC Investigator query: "Show me all wire transfers over $10k where the user changed MFA settings."

-- COMMAND ----------

-- Wire transfers over $10K with MFA change
SELECT
    t.transaction_id,
    t.user_id,
    t.transaction_timestamp,
    t.amount,
    t.merchant_category,
    t.risk_score,
    t.triage_status,
    t.risk_explanation
FROM cmoon_financial_security.fraud_silver.transactions_flagged t
WHERE t.transaction_type = 'wire_transfer'
  AND t.amount > 10000
  AND t.mfa_change_flag = 1
ORDER BY t.risk_score DESC, t.amount DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### KPI 4: Impossible Travel Detection Summary

-- COMMAND ----------

-- Impossible Travel Events
SELECT
    l.user_id,
    l.timestamp AS login_time,
    l.city,
    l.country,
    ROUND(l.distance_miles, 1) AS distance_miles,
    ROUND(l.velocity_mph, 0) AS velocity_mph,
    ROUND(l.time_diff_minutes, 1) AS minutes_between_logins,
    l.ip_address,
    l.mfa_change_flag
FROM cmoon_financial_security.fraud_silver.login_sessions l
WHERE l.impossible_travel_detected = true
ORDER BY l.velocity_mph DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### KPI 5: Bot Detection - Abnormal Typing Cadence

-- COMMAND ----------

-- Bot Detection via Typing Cadence
SELECT
    t.transaction_id,
    t.user_id,
    t.amount,
    t.transaction_type,
    t.typing_cadence_ms,
    t.risk_score,
    t.triage_status,
    t.risk_explanation
FROM cmoon_financial_security.fraud_silver.transactions_flagged t
WHERE t.flag_bot_typing = true
ORDER BY t.typing_cadence_ms ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### KPI 6: Real-Time Fraud Exposure Dashboard

-- COMMAND ----------

-- Fraud Exposure by Category
SELECT
    merchant_category,
    triage_status,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_exposure,
    ROUND(AVG(risk_score), 1) AS avg_risk_score,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS confirmed_fraud
FROM cmoon_financial_security.fraud_silver.transactions_flagged
GROUP BY merchant_category, triage_status
ORDER BY total_exposure DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### KPI 7: Triage Status Overview

-- COMMAND ----------

-- Overall Triage Status Summary
SELECT
    triage_status,
    COUNT(*) AS count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(risk_score), 1) AS avg_risk,
    MIN(risk_score) AS min_risk,
    MAX(risk_score) AS max_risk,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS actual_fraud,
    ROUND(
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2
    ) AS fraud_rate_pct
FROM cmoon_financial_security.fraud_silver.transactions_flagged
GROUP BY triage_status
ORDER BY avg_risk DESC