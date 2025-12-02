# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Pipeline - Ingestion CDC Streaming
# MAGIC
# MAGIC **Pipeline Delta Live Tables pour le layer Bronze**
# MAGIC 
# MAGIC Ce pipeline ingère les données CDC (Change Data Capture) depuis Kafka/Debezium
# MAGIC en temps réel vers les tables Bronze Delta Lake.
# MAGIC
# MAGIC ## Sources de données
# MAGIC - **Kafka Topics**: dbserver1.public.{trip_events, eater, merchant, courier}
# MAGIC - **Format**: Debezium CDC (envelope JSON)
# MAGIC - **Mode**: Streaming continu
# MAGIC
# MAGIC ## Tables créées
# MAGIC - `trip_events_bronze` - Événements de commandes/livraisons
# MAGIC - `eater_bronze` - Clients/consommateurs
# MAGIC - `merchant_bronze` - Restaurants/marchands
# MAGIC - `courier_bronze` - Livreurs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration et Imports

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, from_json, get_json_object, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    TimestampType, DateType, BooleanType
)

# Configuration Kafka - à adapter selon votre environnement
KAFKA_BOOTSTRAP_SERVERS = spark.conf.get("kafka.bootstrap.servers", "kafka:9092")
KAFKA_TOPIC_PREFIX = "dbserver1.public"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schémas CDC Debezium

# COMMAND ----------

# Schéma pour trip_events après extraction du payload Debezium
# Correspond exactement à la table PostgreSQL trip_events
trip_events_schema = StructType([
    StructField("event_id", LongType(), True),
    StructField("trip_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("eater_id", LongType(), True),
    StructField("merchant_id", LongType(), True),
    StructField("courier_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("payload", StringType(), True),  # JSONB payload
    StructField("created_at", TimestampType(), True)
])

# Schéma pour eater - Correspond au schéma PostgreSQL
eater_schema = StructType([
    StructField("eater_id", LongType(), True),
    StructField("eater_uuid", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("address_line_1", StringType(), True),
    StructField("address_line_2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("default_payment_method", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Schéma pour merchant - Correspond au schéma PostgreSQL
merchant_schema = StructType([
    StructField("merchant_id", LongType(), True),
    StructField("merchant_uuid", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("business_type", StringType(), True),
    StructField("cuisine_type", StringType(), True),
    StructField("address_line_1", StringType(), True),
    StructField("address_line_2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("operating_hours", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Schéma pour courier - Correspond au schéma PostgreSQL
courier_schema = StructType([
    StructField("courier_id", LongType(), True),
    StructField("courier_uuid", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("license_plate", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("onboarding_date", DateType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables Bronze - Streaming DLT

# COMMAND ----------

@dlt.table(
    name="trip_events_bronze",
    comment="Table bronze streaming - Événements de commandes et livraisons depuis Kafka/Debezium CDC",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "event_time",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
def trip_events_bronze():
    """
    Ingestion streaming des trip events depuis Kafka.
    Parse l'enveloppe Debezium CDC et extrait payload.after.
    """
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", f"{KAFKA_TOPIC_PREFIX}.trip_events")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", "10000")  # Limite pour micro-batches
            .load()
            .selectExpr(
                "CAST(value AS STRING) as json_str", 
                "timestamp as kafka_timestamp",
                "topic",
                "partition",
                "offset"
            )
            .withColumn("payload_after", get_json_object(col("json_str"), "$.payload.after"))
            .withColumn("parsed", from_json(col("payload_after"), trip_events_schema))
            .select(
                col("parsed.event_id").alias("event_id"),
                col("parsed.trip_id").alias("trip_id"),
                col("parsed.order_id").alias("order_id"),
                col("parsed.eater_id").alias("eater_id"),
                col("parsed.merchant_id").alias("merchant_id"),
                col("parsed.courier_id").alias("courier_id"),
                col("parsed.event_type").alias("event_type"),
                col("parsed.event_time").alias("event_time"),
                col("parsed.payload").alias("payload"),  # JSONB string
                col("parsed.created_at").alias("created_at"),
                col("kafka_timestamp").alias("ingestion_timestamp"),
                col("topic").alias("source_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                current_timestamp().alias("bronze_load_time")
            )
            .withWatermark("event_time", "10 minutes")
    )

# COMMAND ----------

@dlt.table(
    name="eater_bronze",
    comment="Table bronze streaming - Clients/Eaters depuis Kafka/Debezium CDC",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_eater_id", "eater_id IS NOT NULL")
def eater_bronze():
    """
    Ingestion streaming des eaters (clients) depuis Kafka.
    """
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", f"{KAFKA_TOPIC_PREFIX}.eater")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr(
                "CAST(value AS STRING) as json_str", 
                "timestamp as kafka_timestamp"
            )
            .withColumn("payload_after", get_json_object(col("json_str"), "$.payload.after"))
            .withColumn("parsed", from_json(col("payload_after"), eater_schema))
            .select(
                col("parsed.eater_id").alias("eater_id"),
                col("parsed.eater_uuid").alias("eater_uuid"),
                col("parsed.first_name").alias("first_name"),
                col("parsed.last_name").alias("last_name"),
                col("parsed.email").alias("email"),
                col("parsed.phone_number").alias("phone_number"),
                col("parsed.address_line_1").alias("address_line_1"),
                col("parsed.address_line_2").alias("address_line_2"),
                col("parsed.city").alias("city"),
                col("parsed.state_province").alias("state_province"),
                col("parsed.postal_code").alias("postal_code"),
                col("parsed.country").alias("country"),
                col("parsed.default_payment_method").alias("default_payment_method"),
                col("parsed.is_active").alias("is_active"),
                col("parsed.created_at").alias("created_at"),
                col("parsed.updated_at").alias("updated_at"),
                col("kafka_timestamp").alias("ingestion_timestamp"),
                current_timestamp().alias("bronze_load_time")
            )
    )

# COMMAND ----------

@dlt.table(
    name="merchant_bronze",
    comment="Table bronze streaming - Restaurants/Merchants depuis Kafka/Debezium CDC",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
def merchant_bronze():
    """
    Ingestion streaming des merchants (restaurants) depuis Kafka.
    """
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", f"{KAFKA_TOPIC_PREFIX}.merchant")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr(
                "CAST(value AS STRING) as json_str", 
                "timestamp as kafka_timestamp"
            )
            .withColumn("payload_after", get_json_object(col("json_str"), "$.payload.after"))
            .withColumn("parsed", from_json(col("payload_after"), merchant_schema))
            .select(
                col("parsed.merchant_id").alias("merchant_id"),
                col("parsed.merchant_uuid").alias("merchant_uuid"),
                col("parsed.merchant_name").alias("merchant_name"),
                col("parsed.email").alias("email"),
                col("parsed.phone_number").alias("phone_number"),
                col("parsed.business_type").alias("business_type"),
                col("parsed.cuisine_type").alias("cuisine_type"),
                col("parsed.address_line_1").alias("address_line_1"),
                col("parsed.address_line_2").alias("address_line_2"),
                col("parsed.city").alias("city"),
                col("parsed.state_province").alias("state_province"),
                col("parsed.postal_code").alias("postal_code"),
                col("parsed.country").alias("country"),
                col("parsed.operating_hours").alias("operating_hours"),
                col("parsed.is_active").alias("is_active"),
                col("parsed.created_at").alias("created_at"),
                col("parsed.updated_at").alias("updated_at"),
                col("kafka_timestamp").alias("ingestion_timestamp"),
                current_timestamp().alias("bronze_load_time")
            )
    )

# COMMAND ----------

@dlt.table(
    name="courier_bronze",
    comment="Table bronze streaming - Couriers/Livreurs depuis Kafka/Debezium CDC",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_courier_id", "courier_id IS NOT NULL")
def courier_bronze():
    """
    Ingestion streaming des couriers (livreurs) depuis Kafka.
    """
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", f"{KAFKA_TOPIC_PREFIX}.courier")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr(
                "CAST(value AS STRING) as json_str", 
                "timestamp as kafka_timestamp"
            )
            .withColumn("payload_after", get_json_object(col("json_str"), "$.payload.after"))
            .withColumn("parsed", from_json(col("payload_after"), courier_schema))
            .select(
                col("parsed.courier_id").alias("courier_id"),
                col("parsed.courier_uuid").alias("courier_uuid"),
                col("parsed.first_name").alias("first_name"),
                col("parsed.last_name").alias("last_name"),
                col("parsed.email").alias("email"),
                col("parsed.phone_number").alias("phone_number"),
                col("parsed.vehicle_type").alias("vehicle_type"),
                col("parsed.license_plate").alias("license_plate"),
                col("parsed.is_active").alias("is_active"),
                col("parsed.onboarding_date").alias("onboarding_date"),
                col("parsed.created_at").alias("created_at"),
                col("parsed.updated_at").alias("updated_at"),
                col("kafka_timestamp").alias("ingestion_timestamp"),
                current_timestamp().alias("bronze_load_time")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Pipeline
# MAGIC
# MAGIC Pour créer ce pipeline dans Databricks:
# MAGIC ```python
# MAGIC # Dans Databricks UI -> Delta Live Tables -> Create Pipeline
# MAGIC {
# MAGIC   "name": "ubear_bronze_streaming",
# MAGIC   "storage": "/mnt/datalake/ubear/dlt/bronze",
# MAGIC   "target": "ubear_bronze",
# MAGIC   "notebooks": ["pipelines/bronze_pipeline"],
# MAGIC   "configuration": {
# MAGIC     "kafka.bootstrap.servers": "your-kafka-server:9092"
# MAGIC   },
# MAGIC   "continuous": true
# MAGIC }
# MAGIC ```
