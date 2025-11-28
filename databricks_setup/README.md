# Databricks Setup

This folder contains DDL for Delta managed tables. Use this to create environment metadata and to initialize the Lakehouse schema in Databricks.

Files:
- `02_create_tables.sql`: Delta table DDLs for bronze/silver/gold tables and dimensions.

Quick steps:
1. Go to Databricks SQL editor or a notebook, paste `02_create_tables.sql`, and run to create managed Delta tables under `workspace.default`.
2. Make sure Unity Catalog or workspace default catalog is configured.
3. Verify that `workspace.default.trip_events_bronze` and other tables exist before running notebooks.

Notes:
- Tables are managed Delta tables, use `saveAsTable` or Delta SQL `CREATE TABLE` statements.
- Use DBFS for checkpointing streaming jobs, e.g. `dbfs:/tmp/checkpoints/bronze_topic`.
- Use `MERGE INTO` for SCD2 in gold notebooks (done via `03_gold_all_scd2.py`).
