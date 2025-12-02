# uBear Data Warehouse - Déploiement GCP

Guide complet pour déployer l'architecture Data Warehouse sur Google Cloud Platform avec Cloud SQL + Pub/Sub + Databricks.

## 🏗️ Architecture

```
Cloud SQL PostgreSQL (db-f1-micro)
        ↓ (CDC via Debezium)
Google Cloud Pub/Sub (4 topics)
        ↓ (Streaming)
Databricks Delta Live Tables
        ↓
Bronze → Silver → Gold (Lakehouse)
```

## 💰 Coûts Estimés

| Service | Configuration | Coût/mois | Free Tier |
|---------|--------------|-----------|-----------|
| Cloud SQL | db-f1-micro, 10GB | ~$10 | ✅ $300 crédit |
| Pub/Sub | <1M messages/mois | ~$5 | ✅ 10GB gratuit/mois |
| Service Accounts | IAM | Gratuit | ✅ |
| **Total** | | **~$15/mois** | **Gratuit avec crédit** |

> 💡 Avec le **Free Trial GCP ($300)**, tu as **20 mois gratuits** !

## 📋 Prérequis

### 1. Compte GCP
```bash
# Créer un compte: https://console.cloud.google.com
# Activer le Free Trial ($300 de crédit)
```

### 2. Installer gcloud CLI
```bash
# Linux/Mac
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Initialiser
gcloud init
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

### 3. Installer Terraform
```bash
# Linux
wget https://releases.hashicorp.com/terraform/1.6.0/terraform_1.6.0_linux_amd64.zip
unzip terraform_1.6.0_linux_amd64.zip
sudo mv terraform /usr/local/bin/

# Vérifier
terraform version
```

### 4. Installer jq (optionnel)
```bash
sudo apt install jq  # Ubuntu/Debian
brew install jq      # Mac
```

## 🚀 Déploiement Automatique

### Étape 1 : Cloner le repo
```bash
cd ~/Big\ Data\ Projects/BigProjectUbearDw
```

### Étape 2 : Configurer GCP
```bash
# Définir ton projet
export GCP_PROJECT_ID="your-project-id"
gcloud config set project $GCP_PROJECT_ID

# Activer la facturation (requis même pour free tier)
# https://console.cloud.google.com/billing
```

### Étape 3 : Déployer l'infrastructure
```bash
# Rendre le script exécutable
chmod +x deploy_gcp.sh

# Lancer le déploiement
./deploy_gcp.sh
```

Le script va automatiquement :
1. ✅ Activer les APIs GCP nécessaires
2. ✅ Créer Cloud SQL PostgreSQL (db-f1-micro)
3. ✅ Créer 5 topics Pub/Sub (eater, merchant, courier, trip_events, schema)
4. ✅ Créer 4 subscriptions Pub/Sub
5. ✅ Créer 2 Service Accounts (Debezium + Databricks)
6. ✅ Configurer IAM roles
7. ✅ Initialiser la base de données avec les tables
8. ✅ Insérer les données de test

## 🔑 Récupérer les Credentials

### Service Account Debezium
```bash
cd gcp_infrastructure

# Créer la clé JSON
gcloud iam service-accounts keys create debezium-sa-key.json \
  --iam-account=$(terraform output -raw debezium_service_account)

# La clé est dans: ./debezium-sa-key.json
```

### Service Account Databricks
```bash
# Créer la clé JSON
gcloud iam service-accounts keys create databricks-sa-key.json \
  --iam-account=$(terraform output -raw databricks_service_account)

# La clé est dans: ./databricks-sa-key.json
```

## 📝 Configuration Databricks

### Étape 1 : Upload Service Account Key

1. Dans Databricks Workspace, aller à **Settings** → **Compute**
2. Créer un nouveau cluster ou éditer l'existant
3. Dans **Advanced Options** → **Spark Config**, ajouter :
```
gcp.project.id your-gcp-project-id
```

4. Dans **Environment Variables**, ajouter :
```
GOOGLE_APPLICATION_CREDENTIALS=/dbfs/secrets/gcp-sa-key.json
```

5. Uploader la clé :
```python
# Dans un notebook Databricks
dbutils.fs.mkdirs("/dbfs/secrets")
dbutils.fs.put("/dbfs/secrets/gcp-sa-key.json", """
COLLER LE CONTENU DE databricks-sa-key.json ICI
""", True)
```

### Étape 2 : Créer les DLT Pipelines

#### Bronze Pipeline (Pub/Sub → Bronze)
```bash
# Dans Databricks UI:
# 1. Aller à Delta Live Tables
# 2. Cliquer "Create Pipeline"
# 3. Configurer:

Name: ubear-bronze-pubsub-streaming
Notebook: /Repos/ubear-dw/pipelines/bronze_pipeline_pubsub
Storage Location: /tmp/ubear/dlt/bronze
Target: ubear_bronze
Continuous: ✅ Enabled
Photon: ✅ Enabled (si disponible)

# Advanced Configuration:
gcp.project.id: your-gcp-project-id
gcp.service.account.json.path: /dbfs/secrets/gcp-sa-key.json
```

#### Silver Pipeline (Bronze → Silver)
```bash
# Utiliser le pipeline existant silver_pipeline_new.py
# Il lit depuis Bronze, pas besoin de changement
```

#### Gold Pipeline (Silver → Gold)
```bash
# Utiliser le pipeline existant gold_pipeline_complete.py
# Job batch quotidien à 2h UTC
```

### Étape 3 : Installer les dépendances
```python
# Dans le cluster Databricks, installer:
# - google-cloud-pubsub (Maven: com.google.cloud:google-cloud-pubsub:1.120.0)
# - spark-pubsub-connector (Maven: com.google.cloud.spark:spark-pubsub:1.0.0)
```

## 🧪 Tester l'Architecture

### Test 1 : Vérifier Cloud SQL
```bash
# Se connecter à Cloud SQL
gcloud sql connect ubear-postgres-dev --user=foodapp --database=foodapp

# Vérifier les tables
\dt

# Compter les enregistrements
SELECT 'eater' as table, COUNT(*) FROM eater
UNION ALL SELECT 'merchant', COUNT(*) FROM merchant
UNION ALL SELECT 'courier', COUNT(*) FROM courier
UNION ALL SELECT 'trip_events', COUNT(*) FROM trip_events;

\q
```

### Test 2 : Insérer des données et vérifier CDC
```bash
# Insérer un nouvel eater
gcloud sql connect ubear-postgres-dev --user=foodapp --database=foodapp

INSERT INTO eater (eater_uuid, first_name, last_name, email, phone_number, 
                   address_line_1, city, state_province, postal_code, country, 
                   default_payment_method, is_active)
VALUES ('eater-uuid-999', 'Test', 'User', 'test.user@email.com', '+33699999999',
        '999 Test Street', 'Paris', 'Ile-de-France', '75001', 'France',
        'credit_card', true);
```

### Test 3 : Vérifier Pub/Sub
```bash
# Lister les messages dans le topic eater
gcloud pubsub subscriptions pull ubear-eater-sub --limit=5 --auto-ack

# Tu devrais voir le nouveau eater !
```

### Test 4 : Vérifier Bronze Table dans Databricks
```sql
-- Dans un notebook Databricks
SELECT * FROM ubear_bronze.eater_bronze 
WHERE email = 'test.user@email.com';
```

## 🔄 Mettre à jour .env

```bash
# Éditer .env avec les nouvelles valeurs
cd ~/Big\ Data\ Projects/BigProjectUbearDw

# Récupérer les outputs Terraform
cd gcp_infrastructure
terraform output -json > ../terraform_outputs.json
cd ..

# Mettre à jour .env
```

Copier dans `.env` :
```bash
# Cloud SQL
POSTGRES_HOST=<CLOUD_SQL_PUBLIC_IP>
POSTGRES_PORT=5432
POSTGRES_USER=foodapp
POSTGRES_PASSWORD=<FROM_DEPLOYMENT_SUMMARY>
POSTGRES_DB=foodapp

# GCP
GCP_PROJECT_ID=<YOUR_PROJECT_ID>
GCP_SERVICE_ACCOUNT_JSON=/dbfs/secrets/gcp-sa-key.json

# Pub/Sub Topics
PUBSUB_EATER_TOPIC=ubear-eater-cdc
PUBSUB_MERCHANT_TOPIC=ubear-merchant-cdc
PUBSUB_COURIER_TOPIC=ubear-courier-cdc
PUBSUB_TRIP_EVENTS_TOPIC=ubear-trip-events-cdc

# Pub/Sub Subscriptions
PUBSUB_EATER_SUB=projects/<PROJECT_ID>/subscriptions/ubear-eater-sub
PUBSUB_MERCHANT_SUB=projects/<PROJECT_ID>/subscriptions/ubear-merchant-sub
PUBSUB_COURIER_SUB=projects/<PROJECT_ID>/subscriptions/ubear-courier-sub
PUBSUB_TRIP_EVENTS_SUB=projects/<PROJECT_ID>/subscriptions/ubear-trip-events-sub

# Databricks (garder les valeurs existantes)
DATABRICKS_HOST=https://dbc-b9e469a8-62c4.cloud.databricks.com/
DATABRICKS_TOKEN=dapi39299043c10196e11c1b79fe86f5dbdc
```

## 📊 Monitoring

### Vérifier l'état Pub/Sub
```bash
# Topics
gcloud pubsub topics list

# Subscriptions
gcloud pubsub subscriptions list

# Métriques d'un topic
gcloud pubsub topics describe ubear-eater-cdc
```

### Vérifier Cloud SQL
```bash
# État de l'instance
gcloud sql instances describe ubear-postgres-dev

# Logs
gcloud sql operations list --instance=ubear-postgres-dev --limit=10
```

## 🧹 Nettoyage (Détruire l'infrastructure)

⚠️ **ATTENTION** : Cela supprime toutes les ressources et données !

```bash
cd gcp_infrastructure

# Voir ce qui sera détruit
terraform plan -destroy

# Détruire
terraform destroy

# Confirmer avec 'yes'
```

## 🐛 Troubleshooting

### Erreur: API not enabled
```bash
# Activer manuellement les APIs
gcloud services enable sqladmin.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable iam.googleapis.com
```

### Erreur: Insufficient permissions
```bash
# Ajouter le rôle Owner à ton compte
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="user:your-email@gmail.com" \
  --role="roles/owner"
```

### Erreur: Cloud SQL connection timeout
```bash
# Vérifier que l'IP est autorisée
gcloud sql instances patch ubear-postgres-dev \
  --authorized-networks=0.0.0.0/0
```

### Databricks ne peut pas lire Pub/Sub
```bash
# Vérifier les permissions du Service Account
gcloud projects get-iam-policy YOUR_PROJECT_ID \
  --flatten="bindings[].members" \
  --format="table(bindings.role)" \
  --filter="bindings.members:databricks-pubsub-reader@*"
```

## 📚 Ressources

- [Cloud SQL Documentation](https://cloud.google.com/sql/docs)
- [Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
- [Databricks GCP Integration](https://docs.databricks.com/administration-guide/cloud-configurations/gcp/index.html)
- [Terraform GCP Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)

## 🎯 Prochaines Étapes

1. ✅ Infrastructure GCP déployée
2. ⏳ Configurer Debezium pour Pub/Sub (voir `debezium_pubsub_connector.json`)
3. ⏳ Créer les DLT Pipelines dans Databricks
4. ⏳ Tester le flux end-to-end
5. ⏳ Configurer les Jobs Databricks (Gold layer batch)
6. ⏳ Monitoring et alertes

---

**Besoin d'aide ?** Ouvre une issue sur GitHub ! 🚀
