#!/bin/bash
# Script de déploiement automatisé pour uBear Data Warehouse
# Usage: ./deploy.sh [environment]
# Exemple: ./deploy.sh production

set -e  # Exit on error

ENVIRONMENT=${1:-development}
CONFIG_FILE="config.env"

echo "=========================================="
echo "uBear DW - Déploiement Databricks"
echo "Environnement: $ENVIRONMENT"
echo "=========================================="
echo ""

# Vérifier que config.env existe
if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Erreur: Fichier $CONFIG_FILE introuvable"
    echo "📝 Copiez config.env.example vers config.env et configurez vos paramètres"
    exit 1
fi

# Charger la configuration
source $CONFIG_FILE

# Vérifier Databricks CLI
if ! command -v databricks &> /dev/null; then
    echo "❌ Databricks CLI non installé"
    echo "📦 Installation: pip install databricks-cli"
    exit 1
fi

echo "✅ Databricks CLI détecté"

# Configurer Databricks CLI
echo "🔧 Configuration Databricks..."
databricks configure --token <<EOF
$DATABRICKS_HOST
$DATABRICKS_TOKEN
EOF

echo "✅ Configuration terminée"
echo ""

# Étape 1: Créer le catalogue et les schémas
echo "📊 Étape 1/6: Création catalogue et schémas..."
databricks workspace import_dir databricks_setup /tmp/setup_scripts
databricks runs submit --json '{
  "run_name": "Setup Catalog and Schemas",
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "'$GOLD_CLUSTER_NODE_TYPE'",
    "num_workers": 1
  },
  "notebook_task": {
    "notebook_path": "/tmp/setup_scripts/02_create_tables",
    "base_parameters": {
      "catalog": "'$CATALOG_NAME'",
      "bronze_schema": "'$SCHEMA_BRONZE'",
      "silver_schema": "'$SCHEMA_SILVER'",
      "gold_schema": "'$SCHEMA_GOLD'"
    }
  }
}'

echo "✅ Catalogue et schémas créés"
echo ""

# Étape 2: Créer le pipeline Bronze
echo "🥉 Étape 2/6: Création pipeline Bronze..."

cat > /tmp/bronze_pipeline_config.json <<EOF
{
  "name": "ubear_bronze_streaming_$ENVIRONMENT",
  "storage": "$BRONZE_STORAGE_PATH",
  "target": "$SCHEMA_BRONZE",
  "notebooks": [
    {
      "path": "/Repos/ubear-dw/pipelines/bronze_pipeline"
    }
  ],
  "configuration": {
    "kafka.bootstrap.servers": "$KAFKA_BOOTSTRAP_SERVERS",
    "kafka.topic.prefix": "$KAFKA_TOPIC_PREFIX",
    "pipelines.enableTrackHistory": "true"
  },
  "clusters": [
    {
      "label": "default",
      "autoscale": {
        "min_workers": $BRONZE_MIN_WORKERS,
        "max_workers": $BRONZE_MAX_WORKERS,
        "mode": "ENHANCED"
      }
    }
  ],
  "development": false,
  "continuous": $BRONZE_CONTINUOUS,
  "channel": "CURRENT",
  "edition": "ADVANCED"
}
EOF

BRONZE_PIPELINE_ID=$(databricks pipelines create --json-file /tmp/bronze_pipeline_config.json | jq -r '.pipeline_id')
echo "✅ Pipeline Bronze créé: $BRONZE_PIPELINE_ID"
echo ""

# Étape 3: Créer le pipeline Silver
echo "🥈 Étape 3/6: Création pipeline Silver..."

cat > /tmp/silver_pipeline_config.json <<EOF
{
  "name": "ubear_silver_streaming_$ENVIRONMENT",
  "storage": "$SILVER_STORAGE_PATH",
  "target": "$SCHEMA_SILVER",
  "notebooks": [
    {
      "path": "/Repos/ubear-dw/pipelines/silver_pipeline"
    }
  ],
  "configuration": {
    "pipelines.enableTrackHistory": "true"
  },
  "clusters": [
    {
      "label": "default",
      "autoscale": {
        "min_workers": $SILVER_MIN_WORKERS,
        "max_workers": $SILVER_MAX_WORKERS,
        "mode": "ENHANCED"
      }
    }
  ],
  "development": false,
  "continuous": $SILVER_CONTINUOUS,
  "channel": "CURRENT",
  "edition": "ADVANCED",
  "photon": $SILVER_PHOTON_ENABLED
}
EOF

SILVER_PIPELINE_ID=$(databricks pipelines create --json-file /tmp/silver_pipeline_config.json | jq -r '.pipeline_id')
echo "✅ Pipeline Silver créé: $SILVER_PIPELINE_ID"
echo ""

# Étape 4: Créer le job Gold
echo "🥇 Étape 4/6: Création job Gold batch..."

# Substituer les variables dans batch_job.json
envsubst < jobs/batch_job.json > /tmp/batch_job_final.json

GOLD_JOB_ID=$(databricks jobs create --json-file /tmp/batch_job_final.json | jq -r '.job_id')
echo "✅ Job Gold créé: $GOLD_JOB_ID"
echo ""

# Étape 5: Démarrer les pipelines
echo "▶️  Étape 5/6: Démarrage des pipelines..."

echo "Démarrage pipeline Bronze..."
databricks pipelines start --pipeline-id $BRONZE_PIPELINE_ID

echo "Attente 30 secondes..."
sleep 30

echo "Démarrage pipeline Silver..."
databricks pipelines start --pipeline-id $SILVER_PIPELINE_ID

echo "✅ Pipelines démarrés"
echo ""

# Étape 6: Résumé
echo "=========================================="
echo "✨ Déploiement terminé avec succès !"
echo "=========================================="
echo ""
echo "📝 Informations de déploiement:"
echo "   - Environnement: $ENVIRONMENT"
echo "   - Catalogue: $CATALOG_NAME"
echo "   - Pipeline Bronze ID: $BRONZE_PIPELINE_ID"
echo "   - Pipeline Silver ID: $SILVER_PIPELINE_ID"
echo "   - Job Gold ID: $GOLD_JOB_ID"
echo ""
echo "🔗 Liens utiles:"
echo "   - Bronze Pipeline: $DATABRICKS_HOST/#joblist/pipelines/$BRONZE_PIPELINE_ID"
echo "   - Silver Pipeline: $DATABRICKS_HOST/#joblist/pipelines/$SILVER_PIPELINE_ID"
echo "   - Gold Job: $DATABRICKS_HOST/#job/$GOLD_JOB_ID"
echo ""
echo "📊 Prochaines étapes:"
echo "   1. Vérifier que les pipelines Bronze et Silver ingèrent des données"
echo "   2. Exécuter manuellement le job Gold pour la première fois"
echo "   3. Configurer les alertes et le monitoring"
echo "   4. Donner accès aux équipes Analytics/BI"
echo ""
echo "🎉 Votre Data Warehouse est prêt !"
echo ""

# Sauvegarder les IDs pour référence future
cat > deployment_info_$ENVIRONMENT.txt <<EOF
Deployment Date: $(date)
Environment: $ENVIRONMENT
Catalog: $CATALOG_NAME
Bronze Pipeline ID: $BRONZE_PIPELINE_ID
Silver Pipeline ID: $SILVER_PIPELINE_ID
Gold Job ID: $GOLD_JOB_ID
EOF

echo "💾 Informations sauvegardées dans: deployment_info_$ENVIRONMENT.txt"
