# uBear Eats Data Warehouse - Databricks Lakehouse

![Architecture](https://img.shields.io/badge/Platform-Databricks-FF3621?logo=databricks)
![Delta Lake](https://img.shields.io/badge/Storage-Delta_Lake-00ADD8?logo=delta)
![Streaming](https://img.shields.io/badge/Streaming-Kafka-231F20?logo=apache-kafka)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python)

Data Warehouse moderne pour uBear Eats construit sur Databricks Lakehouse avec architecture Medallion (Bronze-Silver-Gold).

## 📋 Table des matières

- [Vue d'ensemble](#vue-densemble)
- [Architecture](#architecture)
- [Structure du projet](#structure-du-projet)
- [Pipelines](#pipelines)
- [Configuration](#configuration)
- [Déploiement](#déploiement)
- [Développement local](#développement-local)
- [Qualité des données](#qualité-des-données)

## 🎯 Vue d'ensemble

Ce Data Warehouse centralise et transforme les données transactionnelles de uBear Eats (plateforme de livraison de nourriture) pour l'analyse et le reporting. Il couvre le parcours complet de la commande depuis le client jusqu'à la livraison.

### Cas d'usage

- **Analyse des performances** : Suivi des métriques de livraison, temps de préparation, satisfaction client
- **Optimisation logistique** : Analyse des zones de livraison, performance des couriers
- **Business Intelligence** : Reporting des ventes, analyse des merchants, comportement clients
- **Data Science** : Modèles de prédiction (temps de livraison, demand forecasting)

### Données sources

| Source | Description | Mode d'ingestion |
|--------|-------------|------------------|
| PostgreSQL `eater` | Données clients | CDC Streaming (Debezium) |
| PostgreSQL `merchant` | Restaurants/marchands | CDC Streaming (Debezium) |
| PostgreSQL `courier` | Livreurs | CDC Streaming (Debezium) |
| PostgreSQL `trip_events` | Événements de commandes | CDC Streaming (Debezium) |

## 🏗️ Architecture

### Architecture Medallion (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SOURCES (PostgreSQL)                        │
│    Eater   │   Merchant   │   Courier   │   Trip Events           │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   Debezium   │ (CDC)
                    │     Kafka    │
                    └──────┬───────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw CDC)                         │
│  - trip_events_bronze    - eater_bronze                            │
│  - merchant_bronze       - courier_bronze                          │
│  Storage: Delta Lake | Mode: Streaming | DLT Pipeline              │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Cleaned & Validated)                │
│  - trip_events_silver    - eater_silver                            │
│  - merchant_silver       - courier_silver                          │
│  Storage: Delta Lake | Mode: Streaming | DLT Pipeline              │
│  Data Quality: Expectations, Deduplication, Parsing                │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   GOLD LAYER (Analytics Ready)                      │
│  DIMENSIONS (SCD Type 2):                                           │
│    - dim_eater          - dim_merchant      - dim_courier          │
│    - dim_date           - dim_time          - dim_location         │
│  FACT TABLE:                                                        │
│    - trip_fact (commandes & livraisons)                            │
│  Storage: Delta Lake | Mode: Batch | Databricks Notebook           │
└─────────────────────────────────────────────────────────────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │   Reporting &   │
                  │   Analytics     │
                  │  (BI Tools)     │
                  └─────────────────┘
```

### Technologies utilisées

- **Platform**: Databricks (AWS/Azure/GCP)
- **Storage**: Delta Lake (ACID transactions, time travel, schema evolution)
- **Streaming**: Apache Kafka + Debezium CDC
- **Processing**: Apache Spark (PySpark)
- **Orchestration**: Databricks Workflows/Jobs
- **Data Quality**: Delta Live Tables Expectations
- **CI/CD**: Git integration avec Databricks Repos

## 📁 Structure du projet

```
uBearDW-databricks-lakehouse/
├── pipelines/                    # Pipelines DLT et notebooks
│   ├── bronze_pipeline.py        # Ingestion CDC streaming (Kafka → Bronze)
│   ├── silver_pipeline.py        # Transformation et nettoyage (Bronze → Silver)
│   └── gold_pipeline.py          # Dimensions SCD2 + Fact table (Silver → Gold)
│
├── jobs/                         # Configurations Databricks Jobs
│   ├── batch_job.json            # Job quotidien Gold layer (2 AM UTC)
│   └── streaming_job.json        # Job streaming continu (Bronze + Silver)
│
├── expectations/                 # Règles de qualité données
│   └── data_quality.py           # Expectations DLT centralisées
│
├── utils/                        # Fonctions utilitaires réutilisables
│   └── transformations.py        # Transformations communes (SCD2, cleaning, etc.)
│
├── databricks_setup/             # Scripts de configuration initiale
│   └── 02_create_tables.sql      # DDL pour création tables Gold
│
├── local_stack/                  # Environnement local de développement
│   ├── docker-compose.yml        # Kafka + Debezium + PostgreSQL
│   ├── initdb/init.sql           # Schéma PostgreSQL initial
│   ├── generate_data.sh          # Script génération données de test
│   └── simulate_cdc.sh           # Simulation événements CDC
│
├── README.md                     # Documentation principale
└── requirements.txt              # Dépendances Python
```

## 🚀 Pipelines

### 1. Bronze Pipeline (Streaming - DLT)

**Fichier**: `pipelines/bronze_pipeline.py`

Ingestion en temps réel des données CDC depuis Kafka vers Delta Lake.

**Tables créées**:
- `trip_events_bronze` - Événements de commandes
- `eater_bronze` - Clients
- `merchant_bronze` - Restaurants
- `courier_bronze` - Livreurs

**Caractéristiques**:
- Mode: Streaming continu
- Source: Kafka (Debezium CDC envelope)
- Format: Delta Lake avec Change Data Feed activé
- Watermark: 10 minutes sur `event_time`
- Expectations: Validation des clés primaires (NOT NULL)

**Démarrage**:
```bash
# Via Databricks UI: Delta Live Tables → Create Pipeline
# Ou via API:
databricks pipelines create --settings bronze_pipeline_config.json
```

### 2. Silver Pipeline (Streaming - DLT)

**Fichier**: `pipelines/silver_pipeline.py`

Transformation et nettoyage des données Bronze vers Silver.

**Transformations appliquées**:
- Parsing du payload JSON (trip_events)
- Nettoyage et normalisation (emails, adresses, postal codes)
- Déduplication
- Calcul des partitions
- Validation de qualité (DLT Expectations)

**Expectations de qualité**:
- Validation emails (format, NOT NULL)
- Validation montants (≥ 0)
- Validation ratings (1-5)
- Validation distances (< 100 miles)

### 3. Gold Pipeline (Batch - Notebook)

**Fichier**: `pipelines/gold_pipeline.py`

Transformation Silver vers Gold avec dimensions SCD2 et table de faits.

**Dimensions SCD Type 2** (historisation complète):
- `dim_eater` - Historique des changements clients
- `dim_merchant` - Historique des changements restaurants
- `dim_courier` - Historique des changements livreurs

**Dimensions statiques**:
- `dim_date` - Calendrier (2020-2030)
- `dim_time` - Heures du jour avec périodes (peak hours)
- `dim_location` - Géographie (à implémenter)

**Table de faits**:
- `trip_fact` - Commandes et livraisons (MERGE upsert sur `trip_id`)

**Exécution**: Job batch quotidien à 2 AM UTC

## ⚙️ Configuration

### Variables d'environnement Databricks

```python
# Configuration à définir dans Databricks Workflows
{
  "kafka.bootstrap.servers": "your-kafka-server:9092",
  "catalog": "ubear_catalog",
  "schema.bronze": "ubear_bronze",
  "schema.silver": "ubear_silver",
  "schema.gold": "ubear_gold"
}
```

### Création du catalogue et schémas

```sql
-- Dans Databricks SQL ou notebook
CREATE CATALOG IF NOT EXISTS ubear_catalog;

CREATE SCHEMA IF NOT EXISTS ubear_catalog.ubear_bronze
  COMMENT 'Raw CDC data from source systems';

CREATE SCHEMA IF NOT EXISTS ubear_catalog.ubear_silver
  COMMENT 'Cleaned and validated data';

CREATE SCHEMA IF NOT EXISTS ubear_catalog.ubear_gold
  COMMENT 'Analytics-ready dimensional model';
```

## 📦 Déploiement

### Prérequis

- Databricks Workspace (AWS/Azure/GCP)
- Kafka cluster avec Debezium CDC configuré
- PostgreSQL source avec réplication logique activée
- Git repo connecté à Databricks Repos

### Étapes de déploiement

#### 1. Configurer Databricks Repos

```bash
# Dans Databricks UI: Repos → Add Repo
# URL: https://github.com/kpatc/uBearDW-databricks-lakehouse
# Branch: main
```

#### 2. Créer les pipelines DLT

**Bronze Pipeline**:
```bash
databricks pipelines create \
  --json '{
    "name": "ubear_bronze_streaming",
    "storage": "/mnt/datalake/ubear/dlt/bronze",
    "target": "ubear_bronze",
    "notebooks": ["/Repos/ubear-dw/pipelines/bronze_pipeline"],
    "configuration": {
      "kafka.bootstrap.servers": "kafka:9092"
    },
    "continuous": true
  }'
```

**Silver Pipeline**:
```bash
databricks pipelines create \
  --json '{
    "name": "ubear_silver_streaming",
    "storage": "/mnt/datalake/ubear/dlt/silver",
    "target": "ubear_silver",
    "notebooks": ["/Repos/ubear-dw/pipelines/silver_pipeline"],
    "continuous": true
  }'
```

#### 3. Créer le job batch Gold

```bash
databricks jobs create --json-file jobs/batch_job.json
```

#### 4. Démarrer les pipelines

```bash
# Démarrer Bronze streaming
databricks pipelines start --pipeline-id <bronze_pipeline_id>

# Démarrer Silver streaming
databricks pipelines start --pipeline-id <silver_pipeline_id>

# Le job batch Gold est schedulé quotidiennement (2 AM UTC)
```

## 🛠️ Développement local

### Setup environnement local avec Docker

```bash
# Démarrer PostgreSQL + Kafka + Debezium
cd local_stack
docker-compose up -d

# Attendre que les services démarrent (30-60 secondes)
sleep 30

# Générer des données de test
./generate_data.sh

# Enregistrer le connecteur Debezium
./register_connector.sh

# Simuler des événements CDC
./simulate_cdc.sh
```

### Tester les pipelines localement

Les pipelines DLT ne peuvent pas s'exécuter localement. Pour le développement:

1. Utiliser Databricks Community Edition (gratuit)
2. Ou tester la logique PySpark dans des notebooks locaux
3. Utiliser `pytest` pour les fonctions dans `utils/`

```bash
# Installer les dépendances
pip install -r requirements.txt

# Exécuter les tests (si configurés)
pytest tests/
```

## 🔍 Qualité des données

### DLT Expectations

Le projet utilise Delta Live Tables Expectations pour garantir la qualité:

**Niveaux de validation**:
- `@dlt.expect()` - Log les violations (métriques)
- `@dlt.expect_or_drop()` - Rejette les enregistrements invalides
- `@dlt.expect_or_fail()` - Fait échouer le pipeline

**Exemple de règles** (voir `expectations/data_quality.py`):
```python
# Silver layer
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
@dlt.expect("valid_ratings", "eater_rating IS NULL OR (eater_rating >= 1 AND eater_rating <= 5)")

# Gold layer
@dlt.expect_or_fail("valid_foreign_keys", "eater_id IS NOT NULL AND merchant_id IS NOT NULL")
```

### Monitoring

Accédez aux métriques de qualité via:
- Databricks DLT Pipeline UI → Data Quality tab
- Event Logs pour violations détaillées
- System tables: `system.dlt.<pipeline>.event_log`

## 📊 Modèle de données Gold

### Star Schema

```
                    ┌──────────────┐
                    │  dim_date    │
                    └──────┬───────┘
                           │
    ┌──────────────┐       │       ┌──────────────┐
    │  dim_eater   │───────┼───────│ dim_merchant │
    │   (SCD2)     │       │       │   (SCD2)     │
    └──────────────┘       │       └──────────────┘
                           │
                    ┌──────┴───────┐
                    │  trip_fact   │
                    └──────┬───────┘
                           │
    ┌──────────────┐       │       ┌──────────────┐
    │  dim_time    │───────┼───────│ dim_courier  │
    └──────────────┘       │       │   (SCD2)     │
                           │       └──────────────┘
                    ┌──────┴───────┐
                    │ dim_location │
                    └──────────────┘
```

### Tables principales

**trip_fact** (Faits):
- Clé: `trip_id` (order_id)
- Mesures: montants (subtotal, delivery_fee, tip, total), métriques temporelles, ratings
- Granularité: Une ligne par commande/livraison
- Partitions: `date_partition`, `region_partition`

**Dimensions SCD2**:
- Historisation complète des changements
- Colonnes SCD2: `effective_start_date`, `effective_end_date`, `is_current`, `version_number`, `row_hash`

## 🤝 Contribution

Pour contribuer au projet:

1. Fork le repository
2. Créer une branche feature (`git checkout -b feature/AmazingFeature`)
3. Commit les changements (`git commit -m 'Add AmazingFeature'`)
4. Push vers la branche (`git push origin feature/AmazingFeature`)
5. Ouvrir une Pull Request

## 📝 License

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.

## 👥 Contact

Data Engineering Team - data-team@ubear.com

Project Link: [https://github.com/kpatc/uBearDW-databricks-lakehouse](https://github.com/kpatc/uBearDW-databricks-lakehouse)

---

**Note**: Ce projet est un exemple d'architecture moderne de Data Warehouse sur Databricks Lakehouse. Adaptez-le selon vos besoins spécifiques.
