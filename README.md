# Little Book of Pipelines — Gradle, Metadata-Driven, DQ-Enforced

This repository implements a improved version of the [Little Book of Pipelines](https://github.com/EcZachly/little-book-of-pipelines) pattern, adapted for **Gradle** multi-module builds and extended to handle real datasets with robust features:

## Key Features
- **Multiple output shapes:** `Record`, `Event`, `Metrics` — no forced single schema
- **Code-first metadata** exported to a **metastore table** (`meta.item_defs_v1`)
- **Data Quality (DQ) enforcement** before writes, driven by metadata
- **Idempotent writes**:
  - MERGE by keys
  - Partitioned by `ds`
  - `_run_id` and `_ingest_ts` fields
  - Deduplication
- Per-job **Spark app name** and structured logging
- **Gradle** multi-project structure for modularity and maintainability

---

## Architecture Overview

```text
             ┌──────────────────────────┐
             │      ItemGroupRegistry    │
             │  (Code-first metadata)    │
             └────────────┬─────────────┘
                          │ ExportMetadata
                          ▼
               ┌──────────────────────────┐
               │ meta.item_defs_v1 table  │
               │ (metastore)              │
               └────────────┬─────────────┘
                          │ Job startup
                          ▼
             ┌──────────────────────────┐
             │  Job Runner (Record/     │
             │  Event/Metrics job)      │
             └────────────┬─────────────┘
                          │ Reads source data
                          ▼
               ┌──────────────────────────┐
               │ Apply transforms          │
               │ Apply DQ rules from meta  │
               └────────────┬─────────────┘
                          │ Valid data only
                          ▼
             ┌──────────────────────────┐
             │  Writer.scala             │
             │  (MERGE / dedupe /        │
             │   partition by ds)        │
             └──────────────────────────┘
```

## 1. Build the Project

```bash
./gradlew clean build
```

## 2. Export Metadata to the Metastore

This step takes the definitions in ItemGroupRegistry and writes them to the table defined in conf/application.conf (default: `meta.item_defs_v1`).
```bash
# Build the fat jar for the runner
./gradlew :runner:shadowJar

# Run the metadata export job
spark-submit \
  --class com.acme.pipes.meta.ExportMetadata \
  modules/runner/build/libs/runner-all.jar
```

## 3. Run Jobs

Each job reads a dataset, applies transformations and DQ checks, then writes to its configured output table.

If `--inputPath` is omitted, jobs default to example paths (e.g. `/mnt/data/*.csv`).

#### CRM Users (Record)
```bash
spark-submit \
  --class com.acme.pipes.jobs.record.CRMUsersJob \
  modules/runner/build/libs/runner-all.jar \
  --inputPath /mnt/data/crm_users.csv \
  --ds 2025-08-08
```

#### POS Transactions (Metrics)
```bash
spark-submit \
  --class com.acme.pipes.jobs.metrics.POSTransactionsJob \
  modules/runner/build/libs/runner-all.jar \
  --inputPath /mnt/data/pos_transactions.csv \
  --ds 2025-08-08
```

#### Web Events (Event)
```bash
spark-submit \
  --class com.acme.pipes.jobs.events.WebEventsJob \
  modules/runner/build/libs/runner-all.jar \
  --inputPath /mnt/data/web_events.csv \
  --ds 2025-08-08
```

#### Notes

- Writer.scala format: Default is parquet. Switch to Delta or Iceberg as required by changing Writer methods.

- `--ds` specifies the logical partition date. Defaults to today if not provided.

- The `meta.item_defs_v1` table must exist in your metastore for jobs to read metadata at runtime.