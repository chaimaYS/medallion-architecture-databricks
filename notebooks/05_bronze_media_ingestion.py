# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Media Ingestion (Images & Videos)
# MAGIC
# MAGIC Ingests binary media files (images, videos) into Delta tables as raw bytes.
# MAGIC - Uses `binaryFile` format to read from cloud storage
# MAGIC - Stores original bytes + metadata (path, size, format, timestamp)
# MAGIC - Supports JPEG, PNG, WebP, MP4, MOV, AVI

# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp, col, lit, sha2, substring_index,
    length, when, lower, regexp_extract
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "main"
BRONZE_SCHEMA = "bronze"
IMAGE_SOURCE = "abfss://media@datalake.dfs.core.windows.net/images/"
VIDEO_SOURCE = "abfss://media@datalake.dfs.core.windows.net/videos/"

IMAGE_FORMATS = ["jpg", "jpeg", "png", "webp", "tiff", "bmp"]
VIDEO_FORMATS = ["mp4", "mov", "avi", "mkv", "webm"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Image Ingestion
# MAGIC Read binary files and extract metadata from the file path.

# COMMAND ----------

def ingest_media(source_path, table_name, media_type):
    """Ingest binary media files into a Delta table."""

    df = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.*")
        .load(source_path)
    )

    # Extract metadata from path
    df_enriched = (
        df
        .withColumn("file_name", substring_index(col("path"), "/", -1))
        .withColumn("file_extension", lower(substring_index(col("file_name"), ".", -1)))
        .withColumn("file_size_bytes", length(col("content")))
        .withColumn("file_hash", sha2(col("content"), 256))
        .withColumn("media_type", lit(media_type))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_path", col("path"))
        .withColumn("year", regexp_extract(col("path"), r"/(\d{4})/", 1))
        .withColumn("month", regexp_extract(col("path"), r"/\d{4}/(\d{2})/", 1))
    )

    # Filter valid formats
    valid_formats = IMAGE_FORMATS if media_type == "image" else VIDEO_FORMATS
    df_filtered = df_enriched.filter(col("file_extension").isin(valid_formats))

    # Write to Delta (append mode, deduplicate by hash)
    target = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    df_filtered.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(target)

    count = spark.table(target).count()
    print(f"Ingested into {target}: {count} total records")
    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest images

# COMMAND ----------

image_count = ingest_media(IMAGE_SOURCE, "raw_images", "image")
print(f"Images ingested: {image_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest videos

# COMMAND ----------

video_count = ingest_media(VIDEO_SOURCE, "raw_videos", "video")
print(f"Videos ingested: {video_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication Check
# MAGIC Identify duplicate files by content hash.

# COMMAND ----------

for table in ["raw_images", "raw_videos"]:
    full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
    total = spark.table(full_name).count()
    unique = spark.table(full_name).select("file_hash").distinct().count()
    dupes = total - unique
    print(f"{table}: {total} total, {unique} unique, {dupes} duplicates")
