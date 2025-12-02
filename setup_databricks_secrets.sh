#!/usr/bin/env bash
# Script de configuration des secrets Databricks pour développement local
# Usage: ./setup_databricks_secrets.sh

set -euo pipefail

# Couleurs pour output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Charger variables d'environnement si .env existe
if [ -f .env ]; then
    echo -e "${GREEN}✓ Chargement .env${NC}"
    export $(cat .env | grep -v '^#' | xargs)
else
    echo -e "${YELLOW}⚠ Fichier .env non trouvé, utilisation des valeurs par défaut${NC}"
fi

# Configuration par défaut
SCOPE_NAME=${DATABRICKS_SECRETS_SCOPE:-"ubear-local-dev"}
KAFKA_SERVERS=${KAFKA_BOOTSTRAP_SERVERS_PUBLIC:-"localhost:29092"}
POSTGRES_HOST=${POSTGRES_HOST:-"localhost"}
POSTGRES_USER=${POSTGRES_USER:-"foodapp"}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-"foodapp"}
POSTGRES_DB=${POSTGRES_DB:-"foodapp"}

echo ""
echo "=========================================="
echo "  Configuration Secrets Databricks"
echo "=========================================="
echo ""

# Vérifier que databricks CLI est installé
if ! command -v databricks &> /dev/null; then
    echo -e "${RED}✗ Databricks CLI n'est pas installé${NC}"
    echo "  Installer avec: pip install databricks-cli"
    exit 1
fi

# Vérifier que databricks est configuré
if [ ! -f ~/.databrickscfg ]; then
    echo -e "${RED}✗ Databricks CLI n'est pas configuré${NC}"
    echo "  Configurer avec: databricks configure --token"
    exit 1
fi

echo -e "${GREEN}✓ Databricks CLI configuré${NC}"
echo ""

# Créer le scope
echo "1️⃣  Création du scope: ${SCOPE_NAME}"
if databricks secrets list-scopes | grep -q "${SCOPE_NAME}"; then
    echo -e "${YELLOW}⚠ Scope ${SCOPE_NAME} existe déjà${NC}"
else
    databricks secrets create-scope --scope "${SCOPE_NAME}" --initial-manage-principal "users"
    echo -e "${GREEN}✓ Scope créé${NC}"
fi
echo ""

# Fonction pour ajouter un secret
add_secret() {
    local key=$1
    local value=$2
    local description=$3
    
    echo "  • ${description}"
    echo "    Clé: ${key}"
    
    # Vérifier si le secret existe déjà
    if databricks secrets list --scope "${SCOPE_NAME}" 2>/dev/null | grep -q "${key}"; then
        echo -n "    Secret existe déjà. Remplacer? [y/N] "
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            echo -e "    ${YELLOW}⊘ Ignoré${NC}"
            return
        fi
    fi
    
    # Ajouter le secret
    echo "${value}" | databricks secrets put-secret --scope "${SCOPE_NAME}" --key "${key}"
    echo -e "    ${GREEN}✓ Secret ajouté${NC}"
}

# Ajouter les secrets Kafka
echo "2️⃣  Configuration Kafka"
add_secret "kafka-bootstrap-servers" "${KAFKA_SERVERS}" "Kafka Bootstrap Servers"
echo ""

# Ajouter les secrets PostgreSQL
echo "3️⃣  Configuration PostgreSQL"
add_secret "postgres-host" "${POSTGRES_HOST}" "PostgreSQL Host"
add_secret "postgres-user" "${POSTGRES_USER}" "PostgreSQL User"
add_secret "postgres-password" "${POSTGRES_PASSWORD}" "PostgreSQL Password"
add_secret "postgres-database" "${POSTGRES_DB}" "PostgreSQL Database"
echo ""

# Lister tous les secrets créés
echo "4️⃣  Secrets créés dans le scope '${SCOPE_NAME}':"
databricks secrets list --scope "${SCOPE_NAME}" | while read -r line; do
    echo "    • $line"
done
echo ""

# Instructions de test
echo "=========================================="
echo "  Configuration terminée !"
echo "=========================================="
echo ""
echo "📝 Pour tester dans un notebook Databricks:"
echo ""
echo "  %python"
echo "  kafka_servers = dbutils.secrets.get(scope=\"${SCOPE_NAME}\", key=\"kafka-bootstrap-servers\")"
echo "  print(f\"Kafka: {kafka_servers}\")"
echo ""
echo "🔐 Les secrets sont maintenant disponibles dans:"
echo "  Scope: ${SCOPE_NAME}"
echo ""
echo "⚠️  IMPORTANT:"
echo "  • Ne jamais afficher les valeurs des secrets en production"
echo "  • Utiliser toujours dbutils.secrets.get() dans le code"
echo "  • Les secrets sont chiffrés au repos"
echo ""
