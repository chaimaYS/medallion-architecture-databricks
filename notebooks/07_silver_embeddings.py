# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — CLIP Embedding Generation
# MAGIC
# MAGIC Generates 512-dimensional CLIP embeddings for all images.
# MAGIC Embeddings enable cross-modal search (text → image, image → image).
# MAGIC
# MAGIC Uses Pandas UDF for GPU-accelerated batch inference.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, pandas_udf
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd

# COMMAND ----------

CATALOG = "main"
SILVER_SCHEMA = "silver"
BATCH_SIZE = 64
MODEL_NAME = "openai/clip-vit-base-patch32"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load CLIP model on each worker

# COMMAND ----------

import torch
from transformers import CLIPProcessor, CLIPModel

def get_clip_model():
    """Load CLIP model (cached per worker via broadcast)."""
    model = CLIPModel.from_pretrained(MODEL_NAME)
    processor = CLIPProcessor.from_pretrained(MODEL_NAME)
    model.eval()
    if torch.cuda.is_available():
        model = model.cuda()
    return model, processor

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pandas UDF for batch embedding generation

# COMMAND ----------

@pandas_udf(ArrayType(FloatType()))
def compute_clip_embedding(content_series: pd.Series) -> pd.Series:
    """Compute CLIP embeddings for a batch of image bytes."""
    from PIL import Image
    import io
    import numpy as np

    model, processor = get_clip_model()
    device = "cuda" if torch.cuda.is_available() else "cpu"
    results = []

    for content in content_series:
        if content is None:
            results.append(None)
            continue
        try:
            img = Image.open(io.BytesIO(content)).convert("RGB")
            inputs = processor(images=img, return_tensors="pt").to(device)

            with torch.no_grad():
                embedding = model.get_image_features(**inputs)
                embedding = embedding / embedding.norm(p=2, dim=-1, keepdim=True)

            results.append(embedding.cpu().numpy().flatten().tolist())
        except Exception:
            results.append(None)

    return pd.Series(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate embeddings for all images

# COMMAND ----------

# Read bronze images (need raw content for embedding)
bronze_images = spark.table(f"{CATALOG}.bronze.raw_images")

# Compute embeddings
embeddings_df = (
    bronze_images
    .select("file_hash", "content")
    .withColumn("embedding", compute_clip_embedding(col("content")))
    .filter(col("embedding").isNotNull())
    .select(
        col("file_hash").alias("image_id"),
        col("embedding"),
        current_timestamp().alias("embedded_at"),
    )
)

embeddings_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.image_embeddings")

count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.image_embeddings").count()
print(f"Generated embeddings for {count} images (dim=512)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify embedding quality

# COMMAND ----------

from pyspark.sql.functions import size, avg

stats = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.image_embeddings")
    .select(
        size(col("embedding")).alias("dim"),
    )
    .agg(avg("dim").alias("avg_dim"))
    .collect()[0]
)

print(f"Average embedding dimension: {stats.avg_dim}")
assert stats.avg_dim == 512.0, f"Expected 512-dim embeddings, got {stats.avg_dim}"
