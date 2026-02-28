# Architecture Decision Record (ADR)

## ADR-001: Medallion Architecture with Delta Lake

**Status:** Accepted  
**Date:** 2026-01-15  
**Author:** Chaima Yedes

---

### Context

The organization needs a scalable, governed data platform to support analytics, reporting, and AI/ML workloads. Data comes from multiple sources (APIs, databases, flat files, CDC streams) with varying quality and formats.

### Decision

Adopt the **Medallion Architecture** (Bronze → Silver → Gold) on **Databricks with Delta Lake** as the core data platform pattern.

### Rationale

| Alternative | Pros | Cons | Decision |
|------------|------|------|----------|
| Medallion (Delta Lake) | ACID, time travel, MERGE, schema evolution, Unity Catalog governance | Databricks dependency | **Selected** |
| Traditional DWH (Synapse) | Familiar SQL, BI-optimized | Limited streaming, no ACID on raw layer | Rejected |
| Data Vault 2.0 | Full auditability, hub/link/satellite | Complexity overhead for current team size | Rejected for now |
| Lambda Architecture | Separate batch + streaming | Dual maintenance, eventual consistency | Rejected |

### Key Design Principles

1. **Bronze is immutable** — raw data is never modified, only appended
2. **Silver is the source of truth** — cleaned, deduplicated, schema-enforced
3. **Gold is consumption-ready** — pre-aggregated for specific use cases
4. **Quality gates between layers** — pipeline halts on critical failures
5. **Incremental by default** — no full reloads unless explicitly needed
6. **Governance-first** — Unity Catalog for lineage, RBAC, and data classification

### Consequences

- All source ingestion must land in Bronze before any transformation
- Silver layer owns the canonical data model (schema contracts enforced)
- Gold tables are purpose-built (one Gold table per use case, not one generic layer)
- Data quality failures at Silver block downstream Gold processing
- Unity Catalog is mandatory for all tables (no unmanaged tables)

---

## ADR-002: Incremental Processing Strategy

**Status:** Accepted  
**Date:** 2026-01-20

### Decision

Use **Delta change tracking + watermark pattern** for all incremental loads rather than full table scans.

### Rationale

- Full reloads are expensive at scale (100M+ rows)
- Delta's `_commit_timestamp` and Change Data Feed provide reliable change tracking
- Watermark pattern (track last processed timestamp) is simple and debuggable
- MERGE INTO handles upserts cleanly without custom logic

### Implementation

- Bronze → Silver: watermark on `_ingestion_timestamp`
- Silver → Gold: full rebuild for aggregation tables (small enough), incremental for fact tables
- CDC sources: use Delta Change Data Feed for row-level tracking

---

## ADR-003: Data Quality Strategy

**Status:** Accepted  
**Date:** 2026-01-22

### Decision

Implement a **three-tier quality check system**: structural (Bronze), semantic (Silver), business (Gold).

### Implementation

| Layer | Check Type | Action on Failure |
|-------|-----------|------------------|
| Bronze | File format, required columns exist | Log warning, continue |
| Silver | Nulls, duplicates, referential integrity, type validation | **Halt pipeline** |
| Gold | Metric consistency, SLA freshness, row count thresholds | Alert team, continue |

### Tools

- **DLT Expectations** for declarative checks in streaming pipelines
- **Custom PySpark checks** for batch pipelines (reusable `DataQualityChecker` class)
- **Unity Catalog tags** for data classification (PII, confidential)
