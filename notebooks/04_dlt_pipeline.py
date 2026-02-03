# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables (DLT) Pipeline
# MAGIC
# MAGIC Declarative pipeline definition for the full medallion flow.
# MAGIC DLT handles orchestration, retries, data quality expectations, and lineage automatically.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit, sha2, concat_ws,
    trim, lower, upper, when, coalesce, row_number,
    sum, count, avg, countDistinct, round as spark_round
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Raw Ingestion

# COMMAND ----------

@dlt.table(
    name="bronze_orders",
    comment="Raw orders ingested from source. Append-only, no transformations.",
    table_properties={"quality": "bronze"}
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://raw@datalake.dfs.core.windows.net/orders/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Cleaned & Validated

# COMMAND ----------

@dlt.table(
    name="silver_orders",
    comment="Cleaned, deduplicated, and validated orders.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_quantity", "quantity >= 0")
@dlt.expect_or_drop("valid_price", "unit_price >= 0")
@dlt.expect("valid_status", "status IN ('pending', 'shipped', 'delivered', 'cancelled', 'processing')")
def silver_orders():
    # Read from Bronze
    df = dlt.read_stream("bronze_orders")
    
    # Deduplicate — keep latest per order_id
    window = Window.partitionBy("order_id").orderBy(col("_ingestion_timestamp").desc())
    
    return (
        df
        .withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
        # Standardize
        .withColumn("region", upper(trim(col("region"))))
        .withColumn("status", lower(trim(col("status"))))
        .withColumn("quantity", coalesce(col("quantity"), lit(0)))
        .withColumn("unit_price", coalesce(col("unit_price"), lit(0.0)))
        .withColumn("total_amount", col("quantity") * col("unit_price"))
        .withColumn("_silver_processed_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Business Aggregations

# COMMAND ----------

@dlt.table(
    name="gold_daily_sales",
    comment="Daily sales summary by region for BI dashboards.",
    table_properties={"quality": "gold"}
)
def gold_daily_sales():
    df = dlt.read("silver_orders")
    
    return (
        df
        .filter(col("status") != "cancelled")
        .groupBy("order_date", "region")
        .agg(
            count("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            spark_round(sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
        )
        .withColumn("_gold_processed_at", current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="gold_customer_360",
    comment="Customer-level profile for segmentation and analytics.",
    table_properties={"quality": "gold"}
)
def gold_customer_360():
    df = dlt.read("silver_orders")
    
    return (
        df
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            spark_round(sum("total_amount"), 2).alias("lifetime_value"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
        )
        .withColumn("customer_segment",
            when(col("lifetime_value") > 10000, "VIP")
            .when(col("lifetime_value") > 5000, "HIGH_VALUE")
            .when(col("total_orders") == 1, "NEW")
            .otherwise("REGULAR")
        )
        .withColumn("_gold_processed_at", current_timestamp())
    )
