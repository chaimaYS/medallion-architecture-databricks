# Medallion Architecture — Databricks + Delta Lake

A production-grade implementation of the **Medallion Architecture** (Bronze → Silver → Gold) on Databricks using Delta Lake, Unity Catalog, and Delta Live Tables.

## Architecture Overview

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│    BRONZE    │────▶│    SILVER    │────▶│     GOLD     │
│  Raw Ingest  │     │   Cleaned    │     │  Aggregated  │
│  Append-only │     │  Validated   │     │  Business    │
│  Schema-on-  │     │  Conformed   │     │  Ready       │
│  read        │     │  Deduplicated│     │  Dimensional │
└──────────────┘     └──────────────┘     └──────────────┘
     ▲                                          │
     │                                          ▼
 Data Sources                            ┌──────────────┐
 (APIs, DBs,                             │  Consumption │
  Files, CDC)                            │  BI / ML / AI│
                                          └──────────────┘
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Platform | Databricks (Unity Catalog) |
| Storage | Delta Lake on ADLS Gen2 |
| Orchestration | Databricks Workflows / Delta Live Tables |
| Transformation | PySpark, Spark SQL |
| Data Quality | DLT Expectations, Custom Checks |
| Governance | Unity Catalog (lineage, RBAC, tagging) |
| CI/CD | Databricks Asset Bundles |
| IaC | Terraform |

## Project Structure

```
medallion-architecture-databricks/
├── README.md
├── notebooks/
│   ├── 01_bronze_ingestion.py        # Raw data ingestion
│   ├── 02_silver_transformation.py   # Cleaning & conforming
│   ├── 03_gold_aggregation.py        # Business-level aggregations
│   └── 04_dlt_pipeline.py            # Delta Live Tables pipeline
├── pipelines/
│   ├── workflow_config.yml           # Databricks Workflow definition
│   └── dlt_pipeline_config.json      # DLT pipeline settings
├── tests/
│   ├── test_data_quality.py          # Data quality checks
│   └── test_transformations.py       # Unit tests for transforms
├── config/
│   ├── schema_contracts.yml          # Schema definitions & contracts
│   └── data_quality_rules.yml        # Quality rules per table
└── docs/
    └── architecture_decision_record.md
```

## Key Design Decisions

### 1. Schema Enforcement & Evolution
- **Bronze**: schema-on-read with `mergeSchema` enabled for flexibility
- **Silver**: strict schema enforcement via contracts
- **Gold**: governed views with RBAC via Unity Catalog

### 2. Data Quality Strategy
- **Bronze**: minimal — only structural validation (file format, required columns exist)
- **Silver**: full quality checks — nulls, duplicates, referential integrity, freshness
- **Gold**: business rule validation — metric consistency, SLA checks

### 3. Incremental Processing
- All layers use **incremental processing** via Delta change tracking
- `MERGE INTO` for Silver upserts with deduplication
- Watermark-based ingestion for streaming/batch sources

### 4. Governance
- Unity Catalog for centralized access control and lineage
- Data classification tags (PII, confidential, public)
- Column-level masking for sensitive fields

## Quick Start

```bash
# Clone the repo
git clone https://github.com/chaimaYS/medallion-architecture-databricks.git

# Import notebooks into Databricks workspace
# Configure Unity Catalog and ADLS connection
# Run the pipeline: 01_bronze → 02_silver → 03_gold
```

## Author

**Chaima Yedes** — Data & AI Architect
- [LinkedIn](https://www.linkedin.com/in/chaima-yedes/)
- yedeschaima5@gmail.com
# Update 2026-01-14
# Update 2026-01-16
# Update 2026-01-23
