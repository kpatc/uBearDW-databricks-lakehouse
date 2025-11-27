# PySpark notebook cell pour Databricks : Ingestion Kafka -> Table Delta managed (bronze)
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

# Schéma du message Kafka (exemple)
schema = StructType() \
    .add("event", StringType()) \
    .add("payload", StringType()) \
    .add("event_time", TimestampType())

# Lecture du topic Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "trip-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Décodage et parsing JSON
value_df = kafka_df.selectExpr("CAST(value AS STRING)")
parsed_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Ecriture dans la table Delta managed (Databricks)
query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/checkpoints/trip_events_bronze") \
    .trigger(processingTime="5 minutes") \
    .toTable("workspace.default.trip_events_bronze")

query.awaitTermination()
