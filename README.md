# Medallion Architecture — Databricks + Delta Lake

A production-grade implementation of the **Medallion Architecture** (Bronze → Silver → Gold) on Databricks. Handles both **structured data** (tables, APIs, CDC) and **unstructured media** (images, videos) in a unified lakehouse pipeline.

## Architecture

```
DATA SOURCES                  BRONZE                  SILVER                   GOLD
─────────────               ──────────              ──────────              ──────────

 APIs / DBs  ──────┐
 CSV / Parquet ────┤       ┌────────────┐      ┌─────────────────┐     ┌──────────────────┐
 CDC streams  ─────┼──────▶│ Raw Tables  │─────▶│ Cleaned Tables   │────▶│ Business Metrics  │
                   │       │ (append-only│      │ (deduped,        │     │ (KPIs, dims,      │
                   │       │  schema-on- │      │  conformed,      │     │  aggregations)    │
                   │       │  read)      │      │  validated)      │     └──────────────────┘
 Images  ──────────┤       ├────────────┤      ├─────────────────┤     ┌──────────────────┐
 Videos  ──────────┘       │ Raw Media   │─────▶│ Media Features   │────▶│ Image Catalog     │
                           │ (binary     │      │ (EXIF, dims,     │     │ Video Library     │
                           │  files +    │      │  thumbnails,     │     │ Media KPIs        │
                           │  metadata)  │      │  CLIP embeddings)│     │ Similarity Index  │
                           └────────────┘      └─────────────────┘     └──────────────────┘
```

## What each layer does

### Bronze — Raw Ingestion
| Data Type | Format | Strategy |
|-----------|--------|----------|
| Structured (CSV, API, CDC) | Delta tables | Append-only, `mergeSchema`, metadata columns |
| Images (JPEG, PNG, WebP) | Delta table with binary column | `binaryFile` reader, content hash for dedup |
| Videos (MP4, MOV, AVI) | Delta table with binary column | `binaryFile` reader, keyframe extraction |

### Silver — Feature Extraction
| Data Type | Processing |
|-----------|-----------|
| Structured | Deduplication, null handling, type casting, schema contracts |
| Images | EXIF extraction (dimensions, GPS, camera), thumbnail generation, CLIP embeddings (512-dim) |
| Videos | Duration, resolution, FPS, codec detection, keyframe extraction (5 per video) |

### Gold — Analytics-Ready
| Output | Description |
|--------|-----------|
| Business metrics | Aggregated KPIs, dimensional models, BI-ready views |
| Image catalog | Searchable image metadata + CLIP embeddings for similarity search |
| Video library | Enriched video metadata with duration categories and quality classification |
| Media KPIs | Collection-wide statistics (count, storage, resolution distribution) |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Platform | Databricks (Unity Catalog) |
| Storage | Delta Lake on ADLS Gen2 |
| Orchestration | Databricks Workflows / Delta Live Tables |
| Transformation | PySpark, Spark SQL |
| Image Processing | Pillow (EXIF, thumbnails), OpenCV (video frames) |
| Embeddings | CLIP ViT-B/32 via HuggingFace Transformers |
| Data Quality | DLT Expectations, custom checks |
| Governance | Unity Catalog (lineage, RBAC, tagging) |
| IaC | Terraform |

## Project Structure

```
├── notebooks/
│   ├── 01_bronze_ingestion.py          # Structured data ingestion
│   ├── 02_silver_transformation.py     # Cleaning & conforming
│   ├── 03_gold_aggregation.py          # Business aggregations
│   ├── 04_dlt_pipeline.py              # Delta Live Tables pipeline
│   ├── 05_bronze_media_ingestion.py    # Image & video binary ingestion
│   ├── 06_silver_media_features.py     # EXIF, thumbnails, video metadata
│   ├── 07_silver_embeddings.py         # CLIP embedding generation (GPU)
│   └── 08_gold_media_analytics.py      # Image catalog, video library, KPIs
├── pipelines/
│   ├── workflow_config.yml
│   └── dlt_pipeline_config.json
├── tests/
│   ├── test_data_quality.py
│   └── test_transformations.py
├── config/
│   ├── schema_contracts.yml
│   └── data_quality_rules.yml
└── docs/
    └── architecture_decision_record.md
```

## Pipeline Execution Order

```
Step 1: Structured data
  01_bronze_ingestion → 02_silver_transformation → 03_gold_aggregation

Step 2: Media data
  05_bronze_media_ingestion → 06_silver_media_features
                            → 07_silver_embeddings
                            → 08_gold_media_analytics

Step 3 (optional): DLT
  04_dlt_pipeline (real-time streaming variant)
```

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Binary storage | Delta tables with binary column | Unified governance via Unity Catalog, no separate object store |
| Image embeddings | CLIP ViT-B/32 (512-dim) | Cross-modal search, no fine-tuning needed |
| Embedding compute | Pandas UDF on GPU cluster | Distributed batch inference, 10x faster than single-node |
| Video keyframes | OpenCV frame extraction | Lightweight, no GPU required for metadata |
| Deduplication | SHA-256 content hash | Byte-level dedup across ingestion batches |
| Thumbnails | 256px JPEG | Fast preview without loading full image |
| Schema strategy | Schema-on-read (Bronze), strict contracts (Silver) | Flexibility for ingestion, reliability for downstream |
| Incremental processing | Delta change tracking + MERGE INTO | Process only new data, reduce compute costs |

## Quick Start

```bash
git clone https://github.com/chaimaYS/medallion-architecture-databricks.git

# Import notebooks into Databricks workspace
# Configure Unity Catalog and ADLS connection
# Run structured pipeline: 01 → 02 → 03
# Run media pipeline: 05 → 06 → 07 → 08
```

## Author

**Chaima Yedes** — Data & AI Architect
- [LinkedIn](https://www.linkedin.com/in/chaima-yedes/)
