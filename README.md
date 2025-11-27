# BigProjectUbearDw - Data Engineering Lakehouse (Databricks-centric)

## Architecture Générale

### 1. Sources de Données (OLTP)
- **PostgreSQL** (ou MySQL/Cassandra) via Docker : simule les bases transactionnelles.

### 2. CDC & Streaming
- **Debezium** (Docker) : capture les changements (CDC) sur la base OLTP.
- **Kafka** (Docker) : bus d'événements (topics : trip-events, user-events, merchant-events).
- **Zookeeper** (Docker) : requis pour Kafka.

### 3. Ingestion & Traitement (100% Databricks)
- **Databricks Notebooks (PySpark)** :
  - Ingestion temps réel Kafka → Delta Lake (bronze) avec Spark Structured Streaming (watermark, checkpoint sur dbfs:/).
  - Traitements batch (silver, gold, SCD2, agrégats) via notebooks et jobs Databricks.
  - Data Quality (Great Expectations) exécutée dans Databricks.
- **Stockage** :
  - Tables Delta managed (bronze, silver, gold) sur DBFS/volumes Unity Catalog.

### 4. Orchestration & DataOps
- **Airflow** (optionnel, Docker ou Databricks Workflows) :
  - Orchestration des jobs (déclenchement des notebooks Databricks via API).
- **CI/CD & DataOps** :
  - Code versionné sur GitHub (scripts, notebooks, configs, tests).
  - Intégration avec Databricks Repos pour synchronisation et exécution directe.
  - Workflows GitHub Actions pour tests, data quality, déploiement.

---

## Dossier Docker minimal (pour simuler la source et le streaming)
- `postgres` (ou autre OLTP)
- `zookeeper`
- `kafka`
- `debezium`

## Dossier Databricks
- Notebooks PySpark (bronze, silver, gold, SCD2, data quality)
- SQL DDL pour les tables Delta managed

## DataOps
- Scripts/tests Great Expectations
- Workflows CI/CD (GitHub Actions)

---

## Flux de Données
1. **OLTP → Debezium → Kafka** (Docker)
2. **Kafka → Databricks (bronze)** (Structured Streaming)
3. **Bronze → Silver → Gold** (Databricks Notebooks/Jobs)
4. **Orchestration, Data Quality, Monitoring** (Airflow, Great Expectations, CI/CD)

---

**Tout le traitement, la qualité, l’orchestration et le stockage sont centralisés dans Databricks. Docker ne sert qu’à simuler la source et le streaming.**
