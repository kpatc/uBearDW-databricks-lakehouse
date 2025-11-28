# Databricks PySpark Notebook: Ingestion Kafka → Bronze (Delta)
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StringType, LongType
from pyspark.sql.functions import from_json, col, expr, get_json_object, lit

# Schéma générique pour trip-events (adapter si besoin)
# Debezium envelope parsing: payload has fields op, after (a map/struct), source.ts_ms
# Generic schema for Debezium 'after' payloads for trip_events table
trip_schema = StructType() \
    .add("order_id", StringType()) \
    .add("eater_id", LongType()) \
    .add("merchant_id", LongType()) \
    .add("courier_id", LongType()) \
    .add("event_time", TimestampType()) \
    .add("payload", StringType())

kafka_bootstrap_servers = "kafka:9092"
topics = [
    "dbserver1.public.trip_events",
    "dbserver1.public.eater",
    "dbserver1.public.merchant",
]
queries = []

for topic in topics:
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    
    value_df = df.selectExpr("CAST(value AS STRING) as json_str")
    # Debezium sends envelope JSON; extract payload.after
    after_json_col = get_json_object(col('json_str'), '$.payload.after')
    # derive base name (table name) from topic (use last segment)
    base_name = topic.split('.')[-1]
    if 'trip_events' in base_name:
        # Parse Debezium after payload into the trip schema
        parsed = from_json(after_json_col, trip_schema)
        parsed_df = parsed.withColumn('topic', lit(topic))
    else:
        # For user-events and merchant-events, write raw envelope payload as a string
        parsed_df = value_df.withColumn('payload', after_json_col).withColumn('topic', lit(topic))
    
    # Debezium wraps row data in payload.after and operation type in payload.op
    # For trip_events parsed_df already has columns order_id, eater_id, merchant_id, courier_id, event_time, payload
    # For other topics parsed_df has string 'payload' column (the after JSON) and 'topic'
    
    # Watermark pour gestion du retard lorsque la colonne existe
    if 'event_time' in parsed_df.columns:
        parsed_df = parsed_df.withWatermark("event_time", "10 minutes")
    
    # Ecriture dans la table bronze (replace '.' and '-' with '_' for table names)
    # Make a friendly table name (last segment plus _bronze)
    table_name = f"{base_name}_bronze".replace('-', '_')
    query = (parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"/dbfs/tmp/checkpoints/bronze_{table_name}")
        .toTable(f"workspace.default.{table_name}")
    )
    
    # Do not await inside the loop; collect queries to wait after starting all of them
    queries.append(query)

# Wait for any streaming queries to terminate
spark.streams.awaitAnyTermination()