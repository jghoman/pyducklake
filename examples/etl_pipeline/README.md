# ETL Pipeline

Simulates a realistic ETL workflow with batch loading, upserts, schema evolution, and partitioning.

## What it demonstrates

- Creating a partitioned table (partition by day)
- Loading data in batches (simulating daily ingestion)
- Upsert for late-arriving corrections
- Schema evolution mid-pipeline (adding a column)
- Filtered queries on the final table

## Prerequisites

- pyducklake installed

## Run

```bash
uv run python examples/etl_pipeline/etl_pipeline.py
```
