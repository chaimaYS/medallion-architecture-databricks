# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Media Feature Extraction
# MAGIC
# MAGIC Extracts structured features from raw media files:
# MAGIC - **Images**: EXIF metadata, dimensions, CLIP embeddings, thumbnail generation
# MAGIC - **Videos**: Duration, resolution, frame rate, keyframe extraction
# MAGIC
# MAGIC Uses Spark UDFs for distributed processing across the cluster.

# COMMAND ----------

from pyspark.sql.functions import (
    col, udf, current_timestamp, lit, when, struct, array
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType,
    ArrayType, BinaryType
)
import io

# COMMAND ----------

CATALOG = "main"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Image Feature Extraction

# COMMAND ----------

# Schema for extracted image features
image_feature_schema = StructType([
    StructField("width", IntegerType()),
    StructField("height", IntegerType()),
    StructField("channels", IntegerType()),
    StructField("format", StringType()),
    StructField("camera_make", StringType()),
    StructField("camera_model", StringType()),
    StructField("gps_lat", FloatType()),
    StructField("gps_lon", FloatType()),
    StructField("taken_at", StringType()),
    StructField("orientation", IntegerType()),
])


@udf(returnType=image_feature_schema)
def extract_image_features(content):
    """Extract metadata and dimensions from image bytes."""
    if content is None:
        return None
    try:
        from PIL import Image
        from PIL.ExifTags import TAGS, GPSTAGS

        img = Image.open(io.BytesIO(content))
        width, height = img.size

        # EXIF metadata
        exif = {}
        exif_data = img.getexif()
        if exif_data:
            for tag_id, value in exif_data.items():
                tag = TAGS.get(tag_id, tag_id)
                if isinstance(value, (str, int, float)):
                    exif[tag] = value

        # GPS coordinates
        gps_lat, gps_lon = None, None
        gps_info = exif_data.get_ifd(0x8825) if exif_data else {}
        if gps_info:
            def dms_to_dd(dms, ref):
                d, m, s = dms
                dd = d + m / 60 + s / 3600
                return -dd if ref in ("S", "W") else dd

            if 2 in gps_info and 4 in gps_info:
                try:
                    gps_lat = dms_to_dd(gps_info[2], gps_info.get(1, "N"))
                    gps_lon = dms_to_dd(gps_info[4], gps_info.get(3, "E"))
                except Exception:
                    pass

        return (
            width, height,
            len(img.getbands()),
            img.format or "unknown",
            exif.get("Make"),
            exif.get("Model"),
            gps_lat, gps_lon,
            str(exif.get("DateTime", "")),
            exif.get("Orientation", 1),
        )
    except Exception:
        return None


@udf(returnType=BinaryType())
def generate_thumbnail(content, max_size=256):
    """Generate a thumbnail from image bytes."""
    if content is None:
        return None
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(content))
        img.thumbnail((max_size, max_size))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=80)
        return buf.getvalue()
    except Exception:
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process images → Silver

# COMMAND ----------

bronze_images = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_images")

silver_images = (
    bronze_images
    .withColumn("features", extract_image_features(col("content")))
    .withColumn("thumbnail", generate_thumbnail(col("content")))
    .select(
        col("file_hash").alias("image_id"),
        col("file_name"),
        col("source_path"),
        col("file_extension").alias("format"),
        col("file_size_bytes"),
        col("features.width"),
        col("features.height"),
        col("features.channels"),
        col("features.camera_make"),
        col("features.camera_model"),
        col("features.gps_lat"),
        col("features.gps_lon"),
        col("features.taken_at"),
        col("features.orientation"),
        col("thumbnail"),
        col("media_type"),
        col("ingested_at"),
        current_timestamp().alias("processed_at"),
    )
)

silver_images.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.images")

print(f"Silver images: {spark.table(f'{CATALOG}.{SILVER_SCHEMA}.images').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Video Feature Extraction

# COMMAND ----------

video_feature_schema = StructType([
    StructField("duration_seconds", FloatType()),
    StructField("width", IntegerType()),
    StructField("height", IntegerType()),
    StructField("fps", FloatType()),
    StructField("codec", StringType()),
    StructField("total_frames", IntegerType()),
])


@udf(returnType=video_feature_schema)
def extract_video_features(content):
    """Extract metadata from video bytes using OpenCV."""
    if content is None:
        return None
    try:
        import cv2
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        cap = cv2.VideoCapture(tmp_path)
        if not cap.isOpened():
            os.unlink(tmp_path)
            return None

        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        duration = total_frames / fps if fps > 0 else 0
        codec_int = int(cap.get(cv2.CAP_PROP_FOURCC))
        codec = "".join([chr((codec_int >> 8 * i) & 0xFF) for i in range(4)])

        cap.release()
        os.unlink(tmp_path)

        return (duration, width, height, fps, codec, total_frames)
    except Exception:
        return None


@udf(returnType=ArrayType(BinaryType()))
def extract_keyframes(content, n_frames=5):
    """Extract evenly-spaced keyframes from video as JPEG bytes."""
    if content is None:
        return None
    try:
        import cv2
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        cap = cv2.VideoCapture(tmp_path)
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total == 0:
            cap.release()
            os.unlink(tmp_path)
            return []

        indices = [int(i * total / n_frames) for i in range(n_frames)]
        frames = []

        for idx in indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
            ret, frame = cap.read()
            if ret:
                _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
                frames.append(buf.tobytes())

        cap.release()
        os.unlink(tmp_path)
        return frames
    except Exception:
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process videos → Silver

# COMMAND ----------

bronze_videos = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_videos")

silver_videos = (
    bronze_videos
    .withColumn("features", extract_video_features(col("content")))
    .withColumn("keyframes", extract_keyframes(col("content"), lit(5)))
    .select(
        col("file_hash").alias("video_id"),
        col("file_name"),
        col("source_path"),
        col("file_extension").alias("format"),
        col("file_size_bytes"),
        col("features.duration_seconds"),
        col("features.width"),
        col("features.height"),
        col("features.fps"),
        col("features.codec"),
        col("features.total_frames"),
        col("keyframes"),
        col("media_type"),
        col("ingested_at"),
        current_timestamp().alias("processed_at"),
    )
)

silver_videos.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.videos")

print(f"Silver videos: {spark.table(f'{CATALOG}.{SILVER_SCHEMA}.videos').count()} rows")
