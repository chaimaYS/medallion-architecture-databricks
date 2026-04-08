# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Media Analytics
# MAGIC
# MAGIC Analytics-ready views combining media features, embeddings, and metadata:
# MAGIC - Image catalog with searchable metadata
# MAGIC - Video library with duration and resolution stats
# MAGIC - Similarity search index for visual retrieval
# MAGIC - Aggregated media KPIs

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, avg, sum as _sum, min as _min, max as _max,
    round as _round, when, lit, current_timestamp,
    concat, date_format, to_date
)

# COMMAND ----------

CATALOG = "main"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Image Catalog
# MAGIC Combines image metadata with embeddings for search-ready access.

# COMMAND ----------

images = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.images")
embeddings = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.image_embeddings")

image_catalog = (
    images
    .join(embeddings, images.image_id == embeddings.image_id, "left")
    .select(
        images.image_id,
        images.file_name,
        images.source_path,
        images.format,
        images.width,
        images.height,
        _round(col("file_size_bytes") / 1024, 1).alias("size_kb"),
        images.camera_make,
        images.camera_model,
        images.gps_lat,
        images.gps_lon,
        when(col("gps_lat").isNotNull(), lit(True)).otherwise(lit(False)).alias("has_gps"),
        when(col("width") >= 3840, "4K")
            .when(col("width") >= 1920, "Full HD")
            .when(col("width") >= 1280, "HD")
            .otherwise("SD").alias("resolution_class"),
        concat(col("width"), lit("x"), col("height")).alias("resolution"),
        embeddings.embedding,
        when(embeddings.embedding.isNotNull(), lit(True)).otherwise(lit(False)).alias("has_embedding"),
        images.processed_at,
        current_timestamp().alias("gold_updated_at"),
    )
)

image_catalog.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.image_catalog")

print(f"Gold image catalog: {image_catalog.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Video Library
# MAGIC Enriched video metadata with duration categories and quality classification.

# COMMAND ----------

videos = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.videos")

video_library = (
    videos
    .select(
        col("video_id"),
        col("file_name"),
        col("source_path"),
        col("format"),
        col("duration_seconds"),
        _round(col("duration_seconds") / 60, 1).alias("duration_minutes"),
        when(col("duration_seconds") < 30, "clip")
            .when(col("duration_seconds") < 300, "short")
            .when(col("duration_seconds") < 1800, "medium")
            .otherwise("long").alias("duration_category"),
        col("width"),
        col("height"),
        concat(col("width"), lit("x"), col("height")).alias("resolution"),
        when(col("width") >= 3840, "4K")
            .when(col("width") >= 1920, "Full HD")
            .when(col("width") >= 1280, "HD")
            .otherwise("SD").alias("resolution_class"),
        col("fps"),
        col("codec"),
        col("total_frames"),
        _round(col("file_size_bytes") / (1024 * 1024), 1).alias("size_mb"),
        col("processed_at"),
        current_timestamp().alias("gold_updated_at"),
    )
)

video_library.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.video_library")

print(f"Gold video library: {video_library.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Media KPIs
# MAGIC Aggregated statistics across the entire media collection.

# COMMAND ----------

image_stats = (
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.image_catalog")
    .agg(
        count("*").alias("total_images"),
        _sum(col("size_kb")).alias("total_size_kb"),
        avg("width").alias("avg_width"),
        avg("height").alias("avg_height"),
        _sum(when(col("has_gps"), 1).otherwise(0)).alias("geotagged_count"),
        _sum(when(col("has_embedding"), 1).otherwise(0)).alias("embedded_count"),
        _sum(when(col("resolution_class") == "4K", 1).otherwise(0)).alias("count_4k"),
        _sum(when(col("resolution_class") == "Full HD", 1).otherwise(0)).alias("count_full_hd"),
    )
    .withColumn("media_type", lit("image"))
)

video_stats = (
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.video_library")
    .agg(
        count("*").alias("total_videos"),
        _sum(col("size_mb")).alias("total_size_mb"),
        avg("duration_seconds").alias("avg_duration_seconds"),
        _sum(col("duration_seconds")).alias("total_duration_seconds"),
        _sum(when(col("resolution_class") == "4K", 1).otherwise(0)).alias("count_4k"),
        avg("fps").alias("avg_fps"),
    )
    .withColumn("media_type", lit("video"))
)

print("=== Image Stats ===")
image_stats.show(truncate=False)

print("=== Video Stats ===")
video_stats.show(truncate=False)
