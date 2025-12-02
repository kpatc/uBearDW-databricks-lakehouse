#!/bin/bash
set -e

echo "📊 Génération de données de test..."

export PGPASSWORD=foodapp

# Exécuter le script SQL
docker exec -i ubear-postgres psql -U foodapp -d foodapp < generate_data.sql

echo ""
echo "✅ Données générées avec succès"
echo ""
echo "📈 Statistiques:"
docker exec ubear-postgres psql -U foodapp -d foodapp -c "
SELECT 
  'eater' as table_name, COUNT(*) as count FROM eater
UNION ALL
SELECT 'merchant', COUNT(*) FROM merchant
UNION ALL
SELECT 'courier', COUNT(*) FROM courier
UNION ALL
SELECT 'trip_events', COUNT(*) FROM trip_events;
"
