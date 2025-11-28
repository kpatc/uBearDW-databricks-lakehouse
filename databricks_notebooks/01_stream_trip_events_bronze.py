# Databricks PySpark Notebook: Ingestion Kafka → Bronze (Delta)
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, expr

# Schéma générique pour trip-events (adapter si besoin)
schema = StructType() \
    .add("event_type", StringType()) \
    .add("event_time", TimestampType()) \
    .add("payload", StringType())

kafka_bootstrap_servers = "kafka:9092"
topics = ["trip-events", "user-events", "merchant-events"]
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
    
    value_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .withColumn("data", from_json(col("json_str"), schema))
    
    parsed_df = value_df.select(
        col("data.event_type"),
        col("data.event_time"),
        col("data.payload"),
        expr(f"'{topic}' as topic")
    )
    
    # Watermark pour gestion du retard
    parsed_df = parsed_df.withWatermark("event_time", "10 minutes")
    
    # Ecriture dans la table bronze (remplace '-' par '_' pour noms de table)
    table_name = topic.replace('-', '_')
    query = (parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"/dbfs/tmp/checkpoints/bronze_{table_name}")
        .toTable(f"workspace.default.{table_name}_bronze")
    )
    
    # Do not await inside the loop; collect queries to wait after starting all of them
    queries.append(query)

# Wait for any streaming queries to terminate
spark.streams.awaitAnyTermination()