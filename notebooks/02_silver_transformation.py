# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleaned, Validated, Conformed
# MAGIC
# MAGIC Transforms Bronze raw data into Silver:
# MAGIC - Deduplication using row hash and watermark
# MAGIC - Data type casting and standardization
# MAGIC - Null handling and default values
# MAGIC - Data quality checks (expectations)
# MAGIC - MERGE (upsert) for incremental loads

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, lower, upper, when, coalesce, lit,
    row_number, current_timestamp, to_date, regexp_replace
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "main"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read from Bronze (Incremental)

# COMMAND ----------

def read_bronze_incremental(table_name: str, watermark_col: str = "_ingestion_timestamp"):
    """
    Read new records from Bronze since last Silver load.
    Uses Delta change tracking for efficient incremental reads.
    """
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
    bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}_raw"
    
    # Get the last processed watermark from Silver
    try:
        last_watermark = (
            spark.table(silver_table)
            .agg({watermark_col: "max"})
            .collect()[0][0]
        )
    except Exception:
        last_watermark = "1900-01-01"
    
    # Read only new records from Bronze
    df = (
        spark.table(bronze_table)
        .filter(col(watermark_col) > last_watermark)
    )
    
    print(f"Incremental read: {df.count()} new records since {last_watermark}")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Deduplication

# COMMAND ----------

def deduplicate(df, key_columns: list, order_column: str = "_ingestion_timestamp"):
    """
    Remove duplicates keeping the latest record per key.
    Uses row_number window function for deterministic dedup.
    """
    window = Window.partitionBy(*key_columns).orderBy(col(order_column).desc())
    
    df_deduped = (
        df
        .withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )
    
    original_count = df.count()
    deduped_count = df_deduped.count()
    print(f"Deduplication: {original_count} → {deduped_count} ({original_count - deduped_count} duplicates removed)")
    
    return df_deduped

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Cleaning & Standardization

# COMMAND ----------

def clean_orders(df):
    """
    Apply business rules for cleaning and standardization.
    - Trim whitespace from strings
    - Standardize status values
    - Handle nulls with defaults
    - Cast data types
    - Filter invalid records
    """
    df_cleaned = (
        df
        # Trim and standardize strings
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("region", upper(trim(col("region"))))
        .withColumn("status", lower(trim(col("status"))))
        
        # Standardize status values
        .withColumn("status", 
            when(col("status").isin("cancelled", "canceled", "cancel"), "cancelled")
            .when(col("status").isin("shipped", "ship", "dispatched"), "shipped")
            .when(col("status").isin("delivered", "complete", "completed"), "delivered")
            .when(col("status").isin("pending", "new", "created"), "pending")
            .otherwise(col("status"))
        )
        
        # Handle nulls
        .withColumn("quantity", coalesce(col("quantity"), lit(0)))
        .withColumn("unit_price", coalesce(col("unit_price"), lit(0.0)))
        .withColumn("region", coalesce(col("region"), lit("UNKNOWN")))
        
        # Derived columns
        .withColumn("total_amount", col("quantity") * col("unit_price"))
        .withColumn("order_date", to_date(col("order_date")))
        
        # Filter out invalid records
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("quantity") >= 0)
        .filter(col("unit_price") >= 0)
        
        # Add processing metadata
        .withColumn("_silver_processed_at", current_timestamp())
    )
    
    return df_cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Data Quality Checks

# COMMAND ----------

def run_quality_checks(df, table_name: str) -> dict:
    """
    Run data quality checks and return results.
    Critical failures will halt the pipeline.
    """
    total_rows = df.count()
    
    checks = {
        "table": table_name,
        "total_rows": total_rows,
        "null_order_id": df.filter(col("order_id").isNull()).count(),
        "null_customer_id": df.filter(col("customer_id").isNull()).count(),
        "negative_quantity": df.filter(col("quantity") < 0).count(),
        "negative_price": df.filter(col("unit_price") < 0).count(),
        "invalid_status": df.filter(~col("status").isin(
            "pending", "shipped", "delivered", "cancelled", "processing"
        )).count(),
        "duplicate_keys": (
            df.groupBy("order_id").count().filter(col("count") > 1).count()
        ),
    }
    
    # Determine pass/fail
    critical_failures = checks["null_order_id"] + checks["null_customer_id"] + checks["duplicate_keys"]
    checks["status"] = "PASS" if critical_failures == 0 else "FAIL"
    
    print(f"Quality Check Results: {checks}")
    
    if checks["status"] == "FAIL":
        raise ValueError(f"Critical data quality failure for {table_name}: {checks}")
    
    return checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: MERGE into Silver (Upsert)

# COMMAND ----------

def merge_to_silver(df, target_table: str, key_columns: list):
    """
    MERGE (upsert) cleaned data into Silver layer.
    - Matching records: update
    - New records: insert
    """
    full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{target_table}"
    
    # Build merge condition
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])
    
    # Check if target table exists
    if spark.catalog.tableExists(full_table_name):
        delta_table = DeltaTable.forName(spark, full_table_name)
        
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"MERGE completed into {full_table_name}")
    else:
        # First load — create table
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_table_name)
        )
        print(f"Initial load into {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Silver Pipeline

# COMMAND ----------

# Step 1: Read incremental from Bronze
df_raw = read_bronze_incremental("orders")

# Step 2: Deduplicate
df_deduped = deduplicate(df_raw, key_columns=["order_id"])

# Step 3: Clean and standardize
df_cleaned = clean_orders(df_deduped)

# Step 4: Quality checks (fails pipeline if critical issues found)
quality_results = run_quality_checks(df_cleaned, "orders")

# Step 5: Merge into Silver
merge_to_silver(df_cleaned, target_table="orders", key_columns=["order_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Load Optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE main.silver.orders ZORDER BY (customer_id, order_date);
