#!/bin/bash
set -e

echo "🔄 Enregistrement du connecteur Debezium..."

# Attendre que Debezium soit prêt
echo "⏳ Attente de Debezium Connect..."
until curl -s http://localhost:8083/ > /dev/null; do
  echo "   En attente..."
  sleep 3
done
echo "✅ Debezium Connect est prêt"

# Supprimer l'ancien connecteur s'il existe
curl -X DELETE http://localhost:8083/connectors/ubear-postgres-connector 2>/dev/null || true

# Créer le nouveau connecteur
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ubear-postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "tasks.max": "1",
      "plugin.name": "pgoutput",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "foodapp",
      "database.password": "foodapp",
      "database.dbname": "foodapp",
      "topic.prefix": "dbserver1",
      "slot.name": "debezium_slot_ubear",
      "publication.name": "dbz_publication_ubear",
      "table.include.list": "public.eater,public.merchant,public.courier,public.trip_events",
      "include.schema.changes": "true",
      "database.history.kafka.bootstrap.servers": "kafka:29092",
      "database.history.kafka.topic": "schema-changes.ubear"
    }
  }'

echo ""
echo "✅ Connecteur enregistré"
echo ""
echo "📊 Vérification du statut:"
curl -s http://localhost:8083/connectors/ubear-postgres-connector/status | jq .

echo ""
echo "📝 Topics Kafka créés (attendre 10 secondes):"
sleep 10
docker exec ubear-kafka kafka-topics --list --bootstrap-server localhost:9092 | grep dbserver1 || echo "   Aucun topic encore (normal si pas de données)"