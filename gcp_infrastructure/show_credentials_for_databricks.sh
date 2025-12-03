#!/bin/bash

# Script pour afficher la clé GCP service account formatée pour Databricks
# =============================================================================

KEY_FILE="databricks-sa-key.json"

echo "=========================================="
echo "Contenu de la clé GCP Service Account"
echo "=========================================="
echo ""
echo "📋 COPIEZ le contenu ci-dessous (de { à }):"
echo ""
echo "=========================================="

if [ -f "$KEY_FILE" ]; then
    # Afficher le JSON en une seule ligne (minified)
    cat "$KEY_FILE" | jq -c .
    echo ""
    echo "=========================================="
    echo ""
    echo "✅ Copiez cette ligne et collez-la dans Databricks"
    echo "   Pipeline Configuration → gcp.credentials.json"
else
    echo "❌ Erreur : Fichier $KEY_FILE introuvable"
    echo "   Assurez-vous d'être dans le dossier gcp_infrastructure/"
    exit 1
fi

echo ""
echo "📌 Instructions :"
echo "1. Sélectionnez et copiez la ligne JSON ci-dessus"
echo "2. Dans Databricks DLT Pipeline Configuration"
echo "3. Ajoutez un paramètre :"
echo "   Key: gcp.credentials.json"
echo "   Value: [COLLEZ LE JSON ICI]"
