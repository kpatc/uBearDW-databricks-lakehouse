# Databricks Notebooks

This folder contains the Databricks PySpark notebooks for the lakehouse pipeline.

Notebooks:
- 01_stream_trip_events_bronze.py — Kafka ingestion to Delta (bronze) with checkpoints on DBFS and watermark.
- 02_silver_cleaning.py — Bronze → Silver transformations: parsing, deduplication, enrichment.
- 03_gold_all_scd2.py — SCD2 merges (MERGE) for dimensions (merchant, eater, courier) and trip_fact MERGE.
- 04_data_quality_trip_fact.py — Great Expectations rules for the trip_fact table.

How to import and run on Databricks:
1. If your repository is connected to Databricks Repos, any push to GitHub will automatically sync into the Databricks Repo. If not, import these files into Databricks Repos or upload them as notebooks.
2. Create a cluster or use an existing cluster and get the cluster_id from the cluster page.
3. Create Databricks Jobs for each notebook (bronze ingestion as a streaming job, silver/gold/data_quality as batch jobs) OR create Jobs that reference notebooks in your Databricks Repos using the `notebook_path`.
4. Set up Airflow to orchestrate notebook runs using the Databricks provider operator. Use the `databricks_default` connection or set your own.

Airflow config (example):
- Use the `DatabricksSubmitRunOperator` to submit notebooks to an existing cluster using `existing_cluster_id`.
- Example cluster_id placeholder: `YOUR_CLUSTER_ID`.
Note: If the repo is already synced via Databricks Repos, importing not needed in CI. See GitHub Actions for optional steps.

Data Quality:
- Configure Great Expectations as a notebook in Databricks and reference Delta tables in `workspace.default`.

Notes:
- Replace `/Repos/your-org/...` paths in DAGs with the actual notebook paths in your workspace.
- Ensure Unity Catalog permissions and Delta table locations are configured for your Databricks workspace.
