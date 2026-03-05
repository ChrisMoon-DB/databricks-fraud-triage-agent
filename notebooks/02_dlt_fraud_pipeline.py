# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Lakeflow Declarative Pipeline: Fraud Detection
# MAGIC Ingests mock transaction and login data from Unity Catalog Volumes into Silver tables.
# MAGIC Joins session biometrics with transaction value and applies real-time fraud detection rules.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, expr, when, lit, abs as spark_abs, sqrt, pow as spark_pow,
    lag, unix_timestamp, window, count, sum as spark_sum, avg,
    to_timestamp, radians, sin, cos, atan2, row_number
)
from pyspark.sql.window import Window

CATALOG = "cmoon_financial_security"
VOLUME_PATH = f"/Volumes/{CATALOG}/fraud_raw/source_files"

# ---------------------------------------------------------------------------
# BRONZE / RAW LAYER - Read CSVs from Volume
# ---------------------------------------------------------------------------

@dlt.table(
    name="transactions_raw",
    comment="Raw transactions ingested from CSV volume",
    table_properties={"quality": "bronze"}
)
def transactions_raw():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/transactions.csv")
        .withColumn("timestamp", to_timestamp("timestamp"))
    )


@dlt.table(
    name="login_logs_raw",
    comment="Raw login logs ingested from CSV volume",
    table_properties={"quality": "bronze"}
)
def login_logs_raw():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/login_logs.csv")
        .withColumn("timestamp", to_timestamp("timestamp"))
    )


@dlt.table(
    name="users_raw",
    comment="Raw user profiles ingested from CSV volume",
    table_properties={"quality": "bronze"}
)
def users_raw():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/users.csv")
    )

# ---------------------------------------------------------------------------
# SILVER LAYER - Enriched & Flagged
# ---------------------------------------------------------------------------

@dlt.table(
    name="users_enriched",
    comment="Users with masked PII columns",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
def users_enriched():
    return dlt.read("users_raw")


@dlt.table(
    name="login_sessions",
    comment="Login sessions enriched with impossible travel detection",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_login", "login_id IS NOT NULL")
@dlt.expect("successful_login", "login_success = 1")
def login_sessions():
    logins = dlt.read("login_logs_raw")

    # Window to detect impossible travel: geolocation jumps > 500 miles in < 10 min
    user_window = Window.partitionBy("user_id").orderBy("timestamp")

    logins_with_prev = logins.withColumn(
        "prev_lat", lag("geolocation_lat").over(user_window)
    ).withColumn(
        "prev_lon", lag("geolocation_lon").over(user_window)
    ).withColumn(
        "prev_timestamp", lag("timestamp").over(user_window)
    )

    # Haversine distance in miles
    enriched = logins_with_prev.withColumn(
        "time_diff_minutes",
        (unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")) / 60
    ).withColumn(
        "dlat", radians(col("geolocation_lat") - col("prev_lat"))
    ).withColumn(
        "dlon", radians(col("geolocation_lon") - col("prev_lon"))
    ).withColumn(
        "a",
        spark_pow(sin(col("dlat") / 2), 2) +
        cos(radians(col("prev_lat"))) * cos(radians(col("geolocation_lat"))) *
        spark_pow(sin(col("dlon") / 2), 2)
    ).withColumn(
        "distance_miles",
        2 * atan2(sqrt(col("a")), sqrt(1 - col("a"))) * 3959  # Earth radius in miles
    ).withColumn(
        "impossible_travel_detected",
        when(
            (col("distance_miles") > 500) & (col("time_diff_minutes") < 10) & (col("time_diff_minutes") > 0),
            True
        ).otherwise(False)
    ).withColumn(
        "velocity_mph",
        when(col("time_diff_minutes") > 0, col("distance_miles") / (col("time_diff_minutes") / 60)).otherwise(0)
    )

    return enriched.select(
        "login_id", "user_id", "timestamp", "ip_address", "city", "country",
        "geolocation_lat", "geolocation_lon", "user_agent", "login_success",
        "mfa_change_flag", "impossible_travel_flag", "typing_cadence_ms",
        "session_id", "impossible_travel_detected", "velocity_mph",
        "distance_miles", "time_diff_minutes"
    )


@dlt.table(
    name="transactions_flagged",
    comment="Transactions joined with login sessions and flagged for fraud indicators",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_transaction", "transaction_id IS NOT NULL")
@dlt.expect("valid_amount", "amount > 0")
def transactions_flagged():
    txns = dlt.read("transactions_raw")
    logins = dlt.read("login_sessions")

    # Get the most recent login per user (to join with transactions)
    latest_login_window = Window.partitionBy("user_id").orderBy(col("timestamp").desc())
    latest_logins = logins.withColumn("rn", row_number().over(latest_login_window)).filter("rn = 1").drop("rn")

    # Join transactions with latest login session
    joined = txns.alias("t").join(
        latest_logins.alias("l"),
        col("t.user_id") == col("l.user_id"),
        "left"
    )

    # Apply fraud detection rules
    flagged = joined.select(
        col("t.transaction_id"),
        col("t.user_id"),
        col("t.timestamp").alias("transaction_timestamp"),
        col("t.amount"),
        col("t.currency"),
        col("t.transaction_type"),
        col("t.merchant_id"),
        col("t.merchant_category"),
        col("t.ip_address"),
        col("t.device_id"),
        col("t.geolocation_lat"),
        col("t.geolocation_lon"),
        col("t.status"),
        col("t.is_fraud"),
        col("l.session_id").alias("login_session_id"),
        col("l.mfa_change_flag"),
        col("l.impossible_travel_detected"),
        col("l.typing_cadence_ms"),
        col("l.velocity_mph"),
    ).withColumn(
        # Rule 1: High-value wire transfer
        "flag_high_value_wire",
        when(
            (col("transaction_type") == "wire_transfer") & (col("amount") > 10000),
            True
        ).otherwise(False)
    ).withColumn(
        # Rule 2: Wire transfer + recent MFA change
        "flag_wire_after_mfa_change",
        when(
            (col("transaction_type") == "wire_transfer") &
            (col("amount") > 10000) &
            (col("mfa_change_flag") == 1),
            True
        ).otherwise(False)
    ).withColumn(
        # Rule 3: Impossible travel detected
        "flag_impossible_travel",
        col("impossible_travel_detected")
    ).withColumn(
        # Rule 4: Unusual typing cadence (potential bot - too fast or too uniform)
        "flag_bot_typing",
        when(col("typing_cadence_ms") < 80, True).otherwise(False)
    ).withColumn(
        # Rule 5: Crypto merchant with high amount
        "flag_crypto_high_value",
        when(
            (col("merchant_category") == "crypto") & (col("amount") > 5000),
            True
        ).otherwise(False)
    ).withColumn(
        # Composite risk score (0-100)
        "risk_score",
        (
            when(col("flag_high_value_wire"), 25).otherwise(0) +
            when(col("flag_wire_after_mfa_change"), 30).otherwise(0) +
            when(col("flag_impossible_travel"), 25).otherwise(0) +
            when(col("flag_bot_typing"), 10).otherwise(0) +
            when(col("flag_crypto_high_value"), 10).otherwise(0)
        )
    ).withColumn(
        # Triage classification
        "triage_status",
        when(col("risk_score") >= 50, "RED_BLOCK")
        .when(col("risk_score") >= 20, "YELLOW_REVIEW")
        .otherwise("GREEN_ALLOW")
    ).withColumn(
        # Human-readable risk explanation (GDPR/CCPA compliant)
        "risk_explanation",
        expr("""
            concat_ws('; ',
                CASE WHEN flag_high_value_wire THEN 'HIGH-VALUE WIRE: Wire transfer exceeding $10,000 threshold' END,
                CASE WHEN flag_wire_after_mfa_change THEN 'MFA CHANGE + WIRE: MFA settings modified before high-value wire transfer — potential account takeover' END,
                CASE WHEN flag_impossible_travel THEN concat('IMPOSSIBLE TRAVEL: User location shifted at ', CAST(ROUND(velocity_mph, 0) AS STRING), ' mph — exceeds physical travel limits') END,
                CASE WHEN flag_bot_typing THEN concat('BOT DETECTION: Typing cadence of ', CAST(typing_cadence_ms AS STRING), 'ms is below human threshold (80ms)') END,
                CASE WHEN flag_crypto_high_value THEN 'CRYPTO HIGH-VALUE: Large transaction to cryptocurrency merchant — elevated laundering risk' END
            )
        """)
    )

    return flagged

# ---------------------------------------------------------------------------
# SERVING / GOLD LAYER - Aggregated KPIs
# ---------------------------------------------------------------------------

@dlt.table(
    name="fraud_kpi_summary",
    comment="Aggregated fraud KPIs for Genie Space and dashboards",
    table_properties={"quality": "gold"}
)
def fraud_kpi_summary():
    txns = dlt.read("transactions_flagged")
    return txns.groupBy("triage_status").agg(
        count("*").alias("total_transactions"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        avg("risk_score").alias("avg_risk_score"),
        spark_sum(when(col("is_fraud") == 1, 1).otherwise(0)).alias("actual_fraud_count"),
        spark_sum(when((col("triage_status") != "GREEN_ALLOW") & (col("is_fraud") == 0), 1).otherwise(0)).alias("false_positive_count")
    )
