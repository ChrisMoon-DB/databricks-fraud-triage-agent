# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - PII Protection & Access Control Setup
# MAGIC Since DLT creates materialized views (which don't support ALTER COLUMN masking),
# MAGIC we create masking functions and secure views in the `fraud_serving` schema
# MAGIC that apply PII protection on top of the silver layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create masking function for card numbers (show last 4 digits only)
# MAGIC CREATE OR REPLACE FUNCTION cmoon_financial_security.fraud_raw.mask_card_number(card_number STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('****-****-****-', RIGHT(card_number, 4));

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create masking function for email addresses
# MAGIC CREATE OR REPLACE FUNCTION cmoon_financial_security.fraud_raw.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT(LEFT(email, 2), '***@', SPLIT(email, '@')[1]);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create masking function for IP addresses (mask last two octets)
# MAGIC CREATE OR REPLACE FUNCTION cmoon_financial_security.fraud_raw.mask_ip(ip_address STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT(
# MAGIC   SPLIT(ip_address, '\\.')[0], '.',
# MAGIC   SPLIT(ip_address, '\\.')[1], '.xxx.xxx'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Secure Views with PII Masking
# MAGIC These views in `fraud_serving` apply masking functions on top of the DLT materialized views in `fraud_silver`.
# MAGIC Analysts and downstream apps should query these views instead of the silver tables directly.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Masked users view
# MAGIC CREATE OR REPLACE VIEW cmoon_financial_security.fraud_serving.users_masked AS
# MAGIC SELECT
# MAGIC   user_id,
# MAGIC   name,
# MAGIC   cmoon_financial_security.fraud_raw.mask_email(email) AS email,
# MAGIC   cmoon_financial_security.fraud_raw.mask_card_number(card_number) AS card_number
# MAGIC FROM cmoon_financial_security.fraud_silver.users_enriched;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Masked transactions view
# MAGIC CREATE OR REPLACE VIEW cmoon_financial_security.fraud_serving.transactions_masked AS
# MAGIC SELECT
# MAGIC   transaction_id,
# MAGIC   user_id,
# MAGIC   transaction_timestamp,
# MAGIC   amount,
# MAGIC   currency,
# MAGIC   transaction_type,
# MAGIC   merchant_id,
# MAGIC   merchant_category,
# MAGIC   cmoon_financial_security.fraud_raw.mask_ip(ip_address) AS ip_address,
# MAGIC   device_id,
# MAGIC   geolocation_lat,
# MAGIC   geolocation_lon,
# MAGIC   status,
# MAGIC   is_fraud,
# MAGIC   login_session_id,
# MAGIC   mfa_change_flag,
# MAGIC   impossible_travel_detected,
# MAGIC   typing_cadence_ms,
# MAGIC   velocity_mph,
# MAGIC   flag_high_value_wire,
# MAGIC   flag_wire_after_mfa_change,
# MAGIC   flag_impossible_travel,
# MAGIC   flag_bot_typing,
# MAGIC   flag_crypto_high_value,
# MAGIC   risk_score,
# MAGIC   triage_status,
# MAGIC   risk_explanation
# MAGIC FROM cmoon_financial_security.fraud_silver.transactions_flagged;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Masked login sessions view
# MAGIC CREATE OR REPLACE VIEW cmoon_financial_security.fraud_serving.login_sessions_masked AS
# MAGIC SELECT
# MAGIC   login_id,
# MAGIC   user_id,
# MAGIC   timestamp,
# MAGIC   cmoon_financial_security.fraud_raw.mask_ip(ip_address) AS ip_address,
# MAGIC   city,
# MAGIC   country,
# MAGIC   geolocation_lat,
# MAGIC   geolocation_lon,
# MAGIC   user_agent,
# MAGIC   login_success,
# MAGIC   mfa_change_flag,
# MAGIC   impossible_travel_flag,
# MAGIC   typing_cadence_ms,
# MAGIC   session_id,
# MAGIC   impossible_travel_detected,
# MAGIC   velocity_mph,
# MAGIC   distance_miles,
# MAGIC   time_diff_minutes
# MAGIC FROM cmoon_financial_security.fraud_silver.login_sessions;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Masking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test masking functions
# MAGIC SELECT
# MAGIC   cmoon_financial_security.fraud_raw.mask_card_number('4532015112830366') AS masked_card,
# MAGIC   cmoon_financial_security.fraud_raw.mask_email('john.doe@bank.com') AS masked_email,
# MAGIC   cmoon_financial_security.fraud_raw.mask_ip('192.168.1.100') AS masked_ip;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify masked users view
# MAGIC SELECT * FROM cmoon_financial_security.fraud_serving.users_masked LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify masked transactions view
# MAGIC SELECT transaction_id, user_id, amount, ip_address, risk_score, triage_status
# MAGIC FROM cmoon_financial_security.fraud_serving.transactions_masked
# MAGIC WHERE triage_status = 'RED_BLOCK'
# MAGIC LIMIT 5;