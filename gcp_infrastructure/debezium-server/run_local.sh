#!/bin/bash

# Script de test local Debezium Server avec Docker
# =============================================================================

set -e

echo "=================================="
echo "Test local Debezium Server"
echo "=================================="

# Vérifier que la clé existe
if [ ! -f "../debezium-sa-key.json" ]; then
    echo "❌ Erreur : debezium-sa-key.json introuvable"
    echo "Exécutez d'abord : gcloud iam service-accounts keys create debezium-sa-key.json ..."
    exit 1
fi

# Build l'image
echo ""
echo "📦 [1/3] Construction de l'image Docker..."
docker build -t debezium-server-ubear:local .

# Créer le volume pour les offsets
echo ""
echo "📁 [2/3] Création du volume pour les offsets..."
docker volume create debezium-offsets 2>/dev/null || true

# Run le container
echo ""
echo "🚀 [3/3] Démarrage du container..."
docker run -d \
    --name debezium-server-ubear \
    -v "$(pwd)/../debezium-sa-key.json:/debezium/secrets/debezium-sa-key.json:ro" \
    -v debezium-offsets:/debezium/data \
    -e GOOGLE_APPLICATION_CREDENTIALS=/debezium/secrets/debezium-sa-key.json \
    -p 8080:8080 \
    debezium-server-ubear:local

echo ""
echo "✅ Container démarré!"
echo ""
echo "Pour voir les logs :"
echo "  docker logs -f debezium-server-ubear"
echo ""
echo "Pour arrêter :"
echo "  docker stop debezium-server-ubear && docker rm debezium-server-ubear"
echo ""
echo "Health check :"
echo "  curl http://localhost:8080/q/health"

# Afficher les logs
sleep 5
echo ""
echo "=================================="
echo "📋 Logs (Ctrl+C pour arrêter)"
echo "=================================="
docker logs -f debezium-server-ubear
