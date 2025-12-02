# Guide de Démarrage Rapide - uBear DW

Ce guide vous permet de déployer rapidement le Data Warehouse uBear Eats sur Databricks.

## ⚡ Prérequis

- ✅ Workspace Databricks (AWS, Azure ou GCP)
- ✅ Kafka cluster avec topics configurés
- ✅ PostgreSQL source avec CDC activé (wal_level=logical)
- ✅ Git repository cloné dans Databricks Repos

## 📝 Étapes de déploiement (30 minutes)

### 1️⃣ Configuration du Workspace (5 min)

#### a) Créer le catalogue et les schémas

```sql
-- Dans un notebook SQL Databricks
CREATE CATALOG IF NOT EXISTS ubear_catalog;
USE CATALOG ubear_catalog;

CREATE SCHEMA IF NOT EXISTS ubear_bronze
  COMMENT 'Raw CDC data from source systems';

CREATE SCHEMA IF NOT EXISTS ubear_silver
  COMMENT 'Cleaned and validated data';

CREATE SCHEMA IF NOT EXISTS ubear_gold
  COMMENT 'Analytics-ready dimensional model';
```

#### b) Configurer les secrets (Databricks CLI)

```bash
# Créer scope pour secrets
databricks secrets create-scope --scope ubear-secrets

# Ajouter secrets Kafka
databricks secrets put --scope ubear-secrets --key kafka-bootstrap-servers
# Entrer: kafka.example.com:9092

# Ajouter secrets PostgreSQL (si nécessaire)
databricks secrets put --scope ubear-secrets --key postgres-password
```

### 2️⃣ Importer le code dans Repos (2 min)

```bash
# Dans Databricks UI: Workspace → Repos → Add Repo
Repository URL: https://github.com/kpatc/uBearDW-databricks-lakehouse
Git provider: GitHub
Branch: main
Path: /Repos/ubear-dw
```

### 3️⃣ Créer le Pipeline Bronze (5 min)

#### Option A: Via UI Databricks

1. Aller à **Delta Live Tables** → **Create Pipeline**
2. Remplir:
   - **Name**: `ubear_bronze_streaming`
   - **Product Edition**: Advanced
   - **Notebook libraries**: `/Repos/ubear-dw/pipelines/bronze_pipeline`
   - **Storage location**: `/mnt/datalake/ubear/dlt/bronze`
   - **Target schema**: `ubear_bronze`
3. Configuration (sous Advanced):
   ```json
   {
     "kafka.bootstrap.servers": "{{secrets/ubear-secrets/kafka-bootstrap-servers}}"
   }
   ```
4. Cluster: Autoscaling 2-8 workers
5. **Pipeline mode**: Continuous
6. Cliquer **Create**

#### Option B: Via API/CLI

```bash
databricks pipelines create --json-file jobs/bronze_pipeline_config.json
```

### 4️⃣ Créer le Pipeline Silver (5 min)

Répéter les étapes ci-dessus avec:
- **Name**: `ubear_silver_streaming`
- **Notebook**: `/Repos/ubear-dw/pipelines/silver_pipeline`
- **Storage**: `/mnt/datalake/ubear/dlt/silver`
- **Target**: `ubear_silver`
- **Pipeline mode**: Continuous

### 5️⃣ Créer les tables Gold (3 min)

```sql
-- Exécuter le script DDL dans un notebook SQL
%sql
-- Copier le contenu de databricks_setup/02_create_tables.sql
-- Ou utiliser le notebook gold_pipeline qui crée les tables automatiquement
```

### 6️⃣ Créer le Job Batch Gold (5 min)

1. Aller à **Workflows** → **Create Job**
2. Importer la configuration:
   ```bash
   # Via CLI
   databricks jobs create --json-file jobs/batch_job.json
   ```
3. Ou créer manuellement:
   - **Name**: `uBear_DW_Batch_Gold_Processing`
   - **Task 1**: Notebook `/Repos/ubear-dw/pipelines/gold_pipeline`
   - **Cluster**: Job cluster (4 workers i3.xlarge)
   - **Schedule**: Cron `0 0 2 * * ?` (2 AM UTC)

### 7️⃣ Démarrer les Pipelines (2 min)

```bash
# Démarrer Bronze (via UI ou CLI)
databricks pipelines start --pipeline-id <bronze_pipeline_id>

# Démarrer Silver
databricks pipelines start --pipeline-id <silver_pipeline_id>

# Le job Gold est schedulé et démarrera automatiquement
```

### 8️⃣ Vérifier le flux de données (3 min)

```sql
-- Vérifier Bronze
SELECT COUNT(*) FROM ubear_bronze.trip_events_bronze;
SELECT COUNT(*) FROM ubear_bronze.eater_bronze;

-- Vérifier Silver (attendre 1-2 min)
SELECT COUNT(*) FROM ubear_silver.trip_events_silver;
SELECT COUNT(*) FROM ubear_silver.eater_silver;

-- Vérifier Gold (après exécution du job)
SELECT COUNT(*) FROM ubear_gold.dim_eater;
SELECT COUNT(*) FROM ubear_gold.trip_fact;
```

## 🧪 Test avec données locales

Si vous voulez tester avec l'environnement local Docker:

```bash
# Depuis le répertoire local_stack/
cd local_stack

# Démarrer les services
docker-compose up -d

# Attendre 30 secondes
sleep 30

# Générer des données
./generate_data.sh

# Enregistrer le connecteur Debezium
./register_connector.sh

# Vérifier que les topics Kafka sont créés
docker exec -it local_stack-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

Ensuite, configurer Databricks pour pointer vers votre Kafka local (ou utiliser un tunnel).

## 🔍 Monitoring

### Vérifier la santé des pipelines

**Bronze/Silver DLT**:
```
Databricks UI → Delta Live Tables → Sélectionner pipeline → Data Quality Tab
```

Métriques clés:
- ✅ Records processed
- ⚠️ Expectations violations
- ❌ Pipeline failures

**Gold Batch Job**:
```
Databricks UI → Workflows → Sélectionner job → Runs
```

Vérifier:
- ✅ Dernière exécution réussie
- ⏱️ Durée d'exécution
- 📊 Records inserted/updated

### Queries de diagnostic

```sql
-- Dernières mises à jour par table
SELECT 
  'trip_fact' as table_name,
  MAX(updated_at) as last_update,
  COUNT(*) as total_records
FROM ubear_gold.trip_fact
UNION ALL
SELECT 
  'dim_eater',
  MAX(effective_start_date),
  COUNT(*)
FROM ubear_gold.dim_eater
WHERE is_current = true;

-- Vérifier la fraîcheur des données
SELECT 
  DATEDIFF(NOW(), MAX(event_time)) as days_old
FROM ubear_silver.trip_events_silver;
-- Doit être < 1 jour

-- Statistiques SCD2
SELECT 
  is_current,
  COUNT(*) as count,
  AVG(version_number) as avg_version
FROM ubear_gold.dim_eater
GROUP BY is_current;
```

## 🐛 Troubleshooting

### Pipeline Bronze ne démarre pas

**Problème**: Erreur de connexion Kafka

**Solution**:
```bash
# Vérifier la configuration Kafka
databricks pipelines get --pipeline-id <id> | grep kafka.bootstrap.servers

# Tester la connexion depuis un notebook
%python
from kafka import KafkaConsumer
consumer = KafkaConsumer(bootstrap_servers='kafka:9092')
print(consumer.topics())
```

### Expectations échouent dans Silver

**Problème**: Trop de données invalides

**Solution**:
```python
# Désactiver temporairement les expectations strictes
# Dans silver_pipeline.py, remplacer:
# @dlt.expect_or_drop(...)
# par:
# @dlt.expect(...)  # Log seulement

# Puis analyser les violations
%sql
SELECT * FROM event_log('<pipeline_id>')
WHERE details:flow_progress.metrics.num_dropped_records > 0
```

### Job Gold prend trop de temps

**Problème**: MERGE lent sur trip_fact

**Solution**:
```sql
-- Augmenter le nombre de workers dans le job
-- Ou optimiser la table avant MERGE
OPTIMIZE ubear_gold.trip_fact
ZORDER BY (trip_id, order_placed_at);

-- Vérifier les statistiques
DESCRIBE DETAIL ubear_gold.trip_fact;
```

## 📚 Ressources supplémentaires

- 📖 [Architecture détaillée](ARCHITECTURE.md)
- 📘 [Documentation complète](README.md)
- 🔧 [Databricks DLT Docs](https://docs.databricks.com/delta-live-tables/)
- 💬 Support: data-team@ubear.com

## ✅ Checklist post-déploiement

- [ ] Pipelines Bronze et Silver en mode continuous
- [ ] Job Gold schedulé quotidien à 2 AM UTC
- [ ] Alertes email configurées (on_failure)
- [ ] Monitoring dashboard créé
- [ ] Documentation équipe mise à jour
- [ ] Accès BI tools configurés (Tableau, Power BI)
- [ ] Tests de bout en bout réussis
- [ ] Backup et disaster recovery plan défini

🎉 **Félicitations ! Votre Data Warehouse uBear est opérationnel !**
