# Databricks Lakehouse & DataOps - Structure du Projet

## 1. Docker (simulation source & streaming)
- `postgres` : base OLTP simulée
- `zookeeper` : pour Kafka
- `kafka` : bus d'événements
- `debezium` : CDC vers Kafka

## 2. Databricks (traitement, stockage, orchestration, data quality)
- Notebooks PySpark :
  - Ingestion Kafka → bronze (streaming)
  - Bronze → Silver (nettoyage, parsing, déduplication)
  - Silver → Gold (SCD2, agrégats, MERGE)
  - Data Quality (Great Expectations)
- SQL DDL : création des tables Delta managed
- Orchestration :
  - Airflow (optionnel) ou Databricks Workflows
  - DAGs déclenchant les notebooks Databricks

## 3. DataOps & CI/CD
- Scripts/tests Great Expectations
- Workflows GitHub Actions (tests, data quality, déploiement)
- Code versionné sur GitHub, synchronisé avec Databricks Repos

## 4. Exemple de structure de repo

```
BigProjectUbearDw/
├── docker-compose.yml
├── docker_etl_setup/
│   └── init/postgres/init.sql
├── databricks_setup/
│   ├── 01_create_folders.py
│   ├── 02_create_tables.sql
│   ├── 03_stream_trip_events_databricks.py
│   ├── 04_silver_cleaning_notebook.py
│   ├── 05_gold_scd2_notebook.py
│   └── data_quality/
│       └── gx_trip_fact_suite.py
├── airflow/
│   └── dags/
│       └── orchestrate_databricks.py
├── .github/
│   └── workflows/
│       └── dataops-ci.yml
└── README.md
```

---

**Tout le traitement, la qualité, l’orchestration et le stockage sont centralisés dans Databricks. Docker ne sert qu’à simuler la source et le streaming.**
