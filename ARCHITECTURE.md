# Architecture uBear Eats Data Warehouse - Databricks Lakehouse

## 🎯 Vue d'ensemble

Ce document décrit l'architecture complète du Data Warehouse uBear Eats construit sur Databricks avec une approche moderne de Lakehouse utilisant Delta Lake et l'architecture Medallion.

## 📊 Architecture Medallion (Bronze → Silver → Gold)

### Bronze Layer (Raw Data)
**Objectif**: Ingestion brute des données CDC en temps réel

- **Source**: PostgreSQL (eater, merchant, courier, trip_events)
- **CDC**: Debezium → Kafka → Databricks
- **Pipeline**: Delta Live Tables (DLT) Streaming
- **Format**: Delta Lake avec Change Data Feed
- **Fichier**: `pipelines/bronze_pipeline.py`

**Tables**:
```
- trip_events_bronze (événements commandes/livraisons)
- eater_bronze (clients)
- merchant_bronze (restaurants)
- courier_bronze (livreurs)
```

**Caractéristiques**:
- Streaming continu (mode continuous)
- Watermark 10 minutes sur event_time
- Preservation de l'enveloppe Debezium CDC
- Validation clés primaires (NOT NULL)

### Silver Layer (Cleaned Data)
**Objectif**: Nettoyage, validation et structuration des données

- **Source**: Tables Bronze
- **Pipeline**: Delta Live Tables (DLT) Streaming
- **Format**: Delta Lake avec Change Data Feed
- **Fichier**: `pipelines/silver_pipeline.py`

**Transformations appliquées**:
1. **Parsing JSON**: Extraction du payload Debezium
2. **Nettoyage**:
   - Normalisation emails (uppercase, trim)
   - Normalisation adresses (trim, postal code cleanup)
   - Normalisation plaques d'immatriculation
3. **Validation**: DLT Expectations pour qualité
4. **Déduplication**: Sur clés métier
5. **Enrichissement**: Calcul partitions (date_partition)

**Tables**:
```
- trip_events_silver (événements nettoyés avec payload parsé)
- eater_silver (clients validés)
- merchant_silver (restaurants nettoyés)
- courier_silver (livreurs validés)
```

**Expectations de qualité**:
- Emails valides (format, NOT NULL)
- Montants positifs (≥ 0)
- Ratings dans plage valide (1-5)
- Distances raisonnables (< 100 miles)

### Gold Layer (Analytics Ready)
**Objectif**: Modèle dimensionnel pour analytics (Star Schema)

- **Source**: Tables Silver
- **Pipeline**: Notebook PySpark Batch
- **Format**: Delta Lake optimisé (Z-Order)
- **Fichier**: `pipelines/gold_pipeline.py`
- **Exécution**: Job batch quotidien (2 AM UTC)

**Dimensions SCD Type 2** (historisation complète):
```
- dim_eater (clients avec historique)
  └─ Colonnes SCD2: effective_start_date, effective_end_date, 
     is_current, version_number, row_hash
  
- dim_merchant (restaurants avec historique)
  └─ Partitionné par: city
  
- dim_courier (livreurs avec historique)
```

**Dimensions statiques**:
```
- dim_date (calendrier 2020-2030)
  └─ Colonnes: date_key, full_date, day_of_week, is_weekend, 
     is_holiday, week_of_year, month, quarter, year
  
- dim_time (1440 minutes par jour)
  └─ Colonnes: time_key, hour_24, hour_12, am_pm, time_period, 
     is_peak_hour
  
- dim_location (géographie - à implémenter)
  └─ Colonnes: latitude, longitude, geohash, h3_index, region_zone
```

**Table de faits**:
```
- trip_fact (commandes et livraisons)
  └─ Clé: trip_id (order_id)
  └─ Mesures: montants, métriques temps, ratings
  └─ Partitionné par: date_partition, region_partition
  └─ Optimisé: Z-Order sur (trip_id, eater_id, merchant_id, order_placed_at)
  └─ MERGE upsert sur trip_id basé sur updated_at
```

## 🔄 Flux de données

```
PostgreSQL (OLTP)
    │
    ▼
Debezium CDC (WAL)
    │
    ▼
Kafka Topics
    │
    ├─ dbserver1.public.trip_events
    ├─ dbserver1.public.eater
    ├─ dbserver1.public.merchant
    └─ dbserver1.public.courier
    │
    ▼
┌─────────────────────────────────────┐
│  BRONZE LAYER (Streaming DLT)       │
│  - trip_events_bronze               │
│  - eater_bronze                     │
│  - merchant_bronze                  │
│  - courier_bronze                   │
│  Mode: Continuous Streaming         │
│  Latency: < 1 minute                │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│  SILVER LAYER (Streaming DLT)       │
│  - trip_events_silver               │
│  - eater_silver                     │
│  - merchant_silver                  │
│  - courier_silver                   │
│  Mode: Continuous Streaming         │
│  Latency: < 2 minutes               │
│  Quality: DLT Expectations          │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│  GOLD LAYER (Batch Notebook)        │
│  DIMENSIONS (SCD2):                 │
│    - dim_eater, dim_merchant        │
│    - dim_courier                    │
│  DIMENSIONS (Static):               │
│    - dim_date, dim_time             │
│  FACTS:                             │
│    - trip_fact (MERGE upsert)       │
│  Schedule: Daily 2 AM UTC           │
│  Duration: ~30-60 minutes           │
└────────────┬────────────────────────┘
             │
             ▼
      BI Tools & Analytics
      (Tableau, Power BI, SQL)
```

## 🛠️ Orchestration

### Jobs Databricks

#### 1. Streaming Job (Continu)
**Fichier**: `jobs/streaming_job.json`

- **Pipeline Bronze**: Ingestion CDC Kafka → Bronze
- **Pipeline Silver**: Transformation Bronze → Silver
- **Mode**: Continuous (24/7)
- **Dépendances**: Bronze doit réussir avant Silver

#### 2. Batch Job (Quotidien)
**Fichier**: `jobs/batch_job.json`

- **Task 1**: Gold Dimensions SCD2 + Trip Fact
- **Task 2**: Optimize Tables (OPTIMIZE, Z-ORDER)
- **Task 3**: Data Quality Checks
- **Schedule**: Cron `0 0 2 * * ?` (2 AM UTC daily)
- **Cluster**: Job Cluster (spot instances avec fallback)

## 📐 Patterns et Best Practices

### 1. SCD Type 2 Implementation

```python
# Logique SCD2 générique (utils/transformations.py)
def apply_scd2_merge(source_df, target_table, business_keys, compare_columns):
    # 1. Calculer row_hash sur compare_columns
    # 2. Joindre source et target sur business_keys
    # 3. Identifier changements (hash différent)
    # 4. Expirer anciens records (is_current=False, effective_end_date=now)
    # 5. Insérer nouveaux records (version_number+1, is_current=True)
```

**Avantages**:
- Historisation complète des changements
- Possibilité de requêter "as of" une date
- Audit trail complet

### 2. Data Quality avec DLT Expectations

```python
# Bronze: Validation stricte des clés
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")

# Silver: Validation métier
@dlt.expect_or_drop("valid_email", "email LIKE '%@%'")
@dlt.expect("valid_ratings", "eater_rating BETWEEN 1 AND 5")

# Gold: Validation intégrité référentielle
@dlt.expect_or_fail("valid_foreign_keys", "eater_id IS NOT NULL")
```

**Niveaux**:
- `expect()`: Log seulement (métriques)
- `expect_or_drop()`: Rejette les invalides
- `expect_or_fail()`: Fait échouer le pipeline

### 3. Partitionnement et Optimisation

**Stratégies de partitionnement**:
```python
# trip_fact: Double partition
.partitionBy("date_partition", "region_partition")

# dim_merchant: Partition par city
.partitionBy("city")

# Z-Order pour queries fréquentes
OPTIMIZE trip_fact ZORDER BY (trip_id, eater_id, merchant_id)
```

**Avantages**:
- Pruning efficace lors des queries
- Performances accrues sur large volume
- Coûts de stockage optimisés

### 4. MERGE Upserts pour Idempotence

```python
# trip_fact: Upsert basé sur updated_at
deltaTable.merge(
    source_df,
    "target.trip_id = source.trip_id"
).whenMatchedUpdate(
    condition="source.updated_at > target.updated_at",
    set={...}
).whenNotMatchedInsertAll().execute()
```

**Avantages**:
- Idempotence (replay safe)
- Gestion des late arrivals
- Pas de duplicates

## 🔒 Sécurité et Gouvernance

### Unity Catalog (Recommandé)

```
Catalog: ubear_catalog
│
├─ Schema: ubear_bronze
│  └─ Tables: *_bronze
│  └─ Access: Data Engineering (READ/WRITE)
│
├─ Schema: ubear_silver
│  └─ Tables: *_silver
│  └─ Access: Data Engineering (READ/WRITE), Analytics (READ)
│
└─ Schema: ubear_gold
   └─ Tables: dim_*, trip_fact
   └─ Access: Analytics (READ), BI Tools (READ)
```

### Row-Level Security (Future)

```sql
-- Example: Restriction par region
CREATE FUNCTION filter_by_region()
RETURN region_partition = current_user_region();

ALTER TABLE trip_fact SET ROW FILTER filter_by_region ON (region_partition);
```

## 📈 Monitoring et Observabilité

### Métriques clés

**Streaming Pipelines (Bronze/Silver)**:
- Input Rate (records/sec)
- Processing Time (latency)
- Data Quality Violations (par expectation)
- Backlog (lag derrière Kafka)

**Batch Pipeline (Gold)**:
- Records Inserted/Updated (par table)
- SCD2 Changes Detected
- Job Duration
- Data Freshness (dernière mise à jour)

### Alerting

```json
{
  "email_notifications": {
    "on_failure": ["data-team@ubear.com"],
    "on_success": ["data-team@ubear.com"]
  }
}
```

## 🚀 Évolutions futures

1. **Near Real-Time Gold**: Remplacer batch par streaming pour trip_fact
2. **ML Features**: Calculer features pour modèles ML
3. **Aggregate Tables**: Tables pré-agrégées pour dashboards
4. **dim_location**: Enrichir avec H3, géohash, zones livraison
5. **Data Retention**: Politique de retention avec VACUUM
6. **CDC Multi-Source**: Ajouter d'autres sources (events app mobile, etc.)

## 📚 Références

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)
- [SCD Type 2 avec Delta Lake](https://docs.databricks.com/delta/merge.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)

---

**Version**: 1.0  
**Dernière mise à jour**: Décembre 2025  
**Maintenu par**: Data Engineering Team
