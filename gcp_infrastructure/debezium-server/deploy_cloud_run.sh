#!/bin/bash

# Script de déploiement Debezium Server sur Google Cloud Run
# =============================================================================

set -e

# Configuration
PROJECT_ID="gentle-voltage-478517-q0"
REGION="europe-west1"
SERVICE_NAME="debezium-server-ubear"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
SERVICE_ACCOUNT="debezium-connector@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=================================="
echo "Déploiement Debezium Server"
echo "=================================="

# Étape 1 : Build l'image Docker
echo ""
echo "📦 [1/5] Construction de l'image Docker..."
cd "$(dirname "$0")"
gcloud builds submit --tag ${IMAGE_NAME} --project=${PROJECT_ID}

# Étape 2 : Créer le secret pour la clé service account
echo ""
echo "🔐 [2/5] Création du secret pour la clé service account..."
if gcloud secrets describe debezium-sa-key --project=${PROJECT_ID} &>/dev/null; then
    echo "Secret existe déjà, mise à jour..."
    gcloud secrets versions add debezium-sa-key --data-file=../debezium-sa-key.json --project=${PROJECT_ID}
else
    echo "Création du secret..."
    gcloud secrets create debezium-sa-key --data-file=../debezium-sa-key.json --project=${PROJECT_ID}
fi

# Donner l'accès au service account
echo "Attribution des permissions sur le secret..."
gcloud secrets add-iam-policy-binding debezium-sa-key \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor" \
    --project=${PROJECT_ID}

# Étape 3 : Déployer sur Cloud Run
echo ""
echo "🚀 [3/5] Déploiement sur Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
    --image ${IMAGE_NAME} \
    --platform managed \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --no-allow-unauthenticated \
    --memory 1Gi \
    --cpu 1 \
    --timeout 3600 \
    --max-instances 1 \
    --min-instances 1 \
    --set-secrets="/debezium/secrets/debezium-sa-key.json=debezium-sa-key:latest" \
    --set-env-vars="GOOGLE_APPLICATION_CREDENTIALS=/debezium/secrets/debezium-sa-key.json" \
    --project=${PROJECT_ID}

# Étape 4 : Vérifier le déploiement
echo ""
echo "✅ [4/5] Vérification du déploiement..."
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} \
    --region ${REGION} \
    --project=${PROJECT_ID} \
    --format='value(status.url)')

echo "Service déployé à : ${SERVICE_URL}"

# Étape 5 : Afficher les logs
echo ""
echo "📋 [5/5] Affichage des logs (Ctrl+C pour arrêter)..."
echo "Pour voir les logs plus tard, utilisez :"
echo "gcloud run services logs read ${SERVICE_NAME} --region ${REGION} --project ${PROJECT_ID}"

echo ""
echo "=================================="
echo "✅ Déploiement terminé avec succès!"
echo "=================================="
echo ""
echo "Service URL: ${SERVICE_URL}"
echo "Service Account: ${SERVICE_ACCOUNT}"
echo "Pub/Sub Topics: ubear-eater-cdc, ubear-merchant-cdc, ubear-courier-cdc, ubear-trip-events-cdc"
echo ""
echo "Pour surveiller les logs :"
echo "gcloud run services logs tail ${SERVICE_NAME} --region ${REGION} --project ${PROJECT_ID}"
