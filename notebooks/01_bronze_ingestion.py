# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Data Ingestion
# MAGIC
# MAGIC Ingests raw data from multiple sources into the Bronze layer.
# MAGIC - Append-only writes
# MAGIC - Schema-on-read (mergeSchema enabled)
# MAGIC - Metadata columns added (ingestion timestamp, source, filename)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, input_file_name, lit, col, sha2, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "main"
BRONZE_SCHEMA = "bronze"
SOURCE_PATH = "abfss://raw@datalake.dfs.core.windows.net/orders/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Schema Definition

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("region", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion — Batch (Auto Loader)

# COMMAND ----------

def ingest_to_bronze(source_path: str, target_table: str, schema: StructType) -> None:
    """
    Ingest raw data into Bronze layer using Auto Loader (cloudFiles).
    Adds metadata columns for lineage and audit.
    """
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"/mnt/checkpoints/{target_table}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("mergeSchema", "true")
        .schema(schema)
        .load(source_path)
    )

    # Add audit/metadata columns
    df_enriched = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_system", lit("orders_api"))
        .withColumn("_row_hash", sha2(concat_ws("||", *[col(c) for c in df.columns]), 256))
    )

    # Write to Bronze — append only, no dedup at this stage
    (
        df_enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"/mnt/checkpoints/{target_table}/data")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{BRONZE_SCHEMA}.{target_table}")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Ingestion

# COMMAND ----------

ingest_to_bronze(
    source_path=SOURCE_PATH,
    target_table="orders_raw",
    schema=orders_schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion — CDC / Streaming Pattern

# COMMAND ----------

def ingest_cdc_to_bronze(kafka_topic: str, target_table: str) -> None:
    """
    Ingest CDC events from Kafka into Bronze layer.
    Preserves the raw CDC envelope (op, ts_ms, before, after).
    """
    df_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka-broker:9092")
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    df_parsed = (
        df_stream
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_system", lit(f"kafka_{kafka_topic}"))
    )

    (
        df_parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"/mnt/checkpoints/{target_table}/cdc")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{BRONZE_SCHEMA}.{target_table}")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Ingestion Validation

# COMMAND ----------

def validate_bronze(table_name: str) -> dict:
    """Basic structural validation after ingestion."""
    df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}")
    
    row_count = df.count()
    null_keys = df.filter(col("order_id").isNull()).count()
    latest_ingestion = df.agg({"_ingestion_timestamp": "max"}).collect()[0][0]
    
    validation = {
        "table": table_name,
        "row_count": row_count,
        "null_key_count": null_keys,
        "latest_ingestion": str(latest_ingestion),
        "status": "PASS" if null_keys == 0 and row_count > 0 else "FAIL"
    }
    
    print(f"Bronze Validation: {validation}")
    return validation

validate_bronze("orders_raw")
