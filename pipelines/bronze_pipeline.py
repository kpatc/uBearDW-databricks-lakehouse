"""
Bronze Pipeline pour Databricks avec Apache Kafka
Compatible avec Databricks Serverless
CDC ingestion depuis Debezium Server
"""

import dlt
from pyspark.sql.functions import from_json, col, struct, to_timestamp, cast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType

# =============================================================================
# Schemas Debezium CDC
# =============================================================================

# Schema pour le payload Debezium (after/before)
eater_payload_schema = StructType([
    StructField("eater_id", IntegerType(), True),
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
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True)
])

merchant_payload_schema = StructType([
    StructField("merchant_id", IntegerType(), True),
    StructField("merchant_uuid", StringType(), True),
    StructField("name", StringType(), True),
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
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True)
])

courier_payload_schema = StructType([
    StructField("courier_id", IntegerType(), True),
    StructField("courier_uuid", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("license_plate", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("onboarding_date", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True)
])

trip_events_payload_schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("trip_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("actor_type", StringType(), True),
    StructField("actor_id", IntegerType(), True),
    StructField("event_details", StringType(), True),
    StructField("created_at", LongType(), True)
])

# Schema Debezium envelope complet
debezium_envelope_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StringType(), True),  # Peut être n'importe quel schema
        StructField("after", StringType(), True),
        StructField("source", StructType([
            StructField("version", StringType(), True),
            StructField("connector", StringType(), True),
            StructField("name", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True)
        ]), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True)
    ]), True)
])

# =============================================================================
# Helper function pour lire depuis Kafka
# =============================================================================

def read_kafka_topic(topic_name):
    """Lit un topic Kafka et retourne un DataFrame streaming"""
    kafka_servers = spark.conf.get("kafka.bootstrap.servers")
    
    # SASL credentials for Confluent Cloud
    sasl_jaas_config = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="HYPO6LDVPLC2EAYE" password="cfltDnW60V6dsBmYpBFAjnAaQq+lA70I7tim4/XLDCftn0jMqMxAfx4AxdaiC9Iw";'
    
    # Configuration Confluent Cloud SASL_SSL
    # Note: kafka.admin.* options not supported on Databricks Serverless/Shared clusters
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        # Confluent Cloud Authentication
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", sasl_jaas_config)
        .load()
    )

# =============================================================================
# Bronze Tables - CDC Ingestion
# =============================================================================

@dlt.table(
    name="eater_bronze",
    comment="Raw CDC data for eaters from Kafka",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "eater_id"}
)
def eater_bronze():
    """Ingestion streaming depuis Kafka topic ubear.public.eater"""
    
    raw_stream = read_kafka_topic("ubear.public.eater")
    
    # Parser le message Kafka
    parsed_stream = raw_stream.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp"
    ).withColumn(
        # Parser le JSON Debezium envelope
        "payload_parsed",
        from_json(col("raw_json"), debezium_envelope_schema)
    ).withColumn(
        # Extraire le payload "after" (les données)
        "after_parsed",
        from_json(col("payload_parsed.payload.after"), eater_payload_schema)
    ).select(
        # Données principales (after state)
        col("after_parsed.eater_id").alias("eater_id"),
        col("after_parsed.eater_uuid").alias("eater_uuid"),
        col("after_parsed.first_name").alias("first_name"),
        col("after_parsed.last_name").alias("last_name"),
        col("after_parsed.email").alias("email"),
        col("after_parsed.phone_number").alias("phone_number"),
        col("after_parsed.address_line_1").alias("address_line_1"),
        col("after_parsed.address_line_2").alias("address_line_2"),
        col("after_parsed.city").alias("city"),
        col("after_parsed.state_province").alias("state_province"),
        col("after_parsed.postal_code").alias("postal_code"),
        col("after_parsed.country").alias("country"),
        col("after_parsed.default_payment_method").alias("default_payment_method"),
        col("after_parsed.is_active").alias("is_active"),
        
        # Timestamps
        to_timestamp(col("after_parsed.created_at") / 1000).alias("created_at"),
        to_timestamp(col("after_parsed.updated_at") / 1000).alias("updated_at"),
        
        # CDC metadata
        col("payload_parsed.payload.op").alias("cdc_operation"),
        to_timestamp(col("payload_parsed.payload.ts_ms") / 1000).alias("cdc_timestamp"),
        col("payload_parsed.payload.source.snapshot").alias("cdc_snapshot"),
        
        # Kafka metadata
        col("kafka_key").alias("kafka_key"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("kafka_timestamp").alias("kafka_timestamp")
    )
    
    return parsed_stream


@dlt.table(
    name="merchant_bronze",
    comment="Raw CDC data for merchants from Kafka",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "merchant_id"}
)
def merchant_bronze():
    """Ingestion streaming depuis Kafka topic ubear.public.merchant"""
    
    raw_stream = read_kafka_topic("ubear.public.merchant")
    
    parsed_stream = raw_stream.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp"
    ).withColumn(
        "payload_parsed",
        from_json(col("raw_json"), debezium_envelope_schema)
    ).withColumn(
        "after_parsed",
        from_json(col("payload_parsed.payload.after"), merchant_payload_schema)
    ).select(
        col("after_parsed.merchant_id").alias("merchant_id"),
        col("after_parsed.merchant_uuid").alias("merchant_uuid"),
        col("after_parsed.name").alias("name"),
        col("after_parsed.email").alias("email"),
        col("after_parsed.phone_number").alias("phone_number"),
        col("after_parsed.business_type").alias("business_type"),
        col("after_parsed.cuisine_type").alias("cuisine_type"),
        col("after_parsed.address_line_1").alias("address_line_1"),
        col("after_parsed.address_line_2").alias("address_line_2"),
        col("after_parsed.city").alias("city"),
        col("after_parsed.state_province").alias("state_province"),
        col("after_parsed.postal_code").alias("postal_code"),
        col("after_parsed.country").alias("country"),
        col("after_parsed.operating_hours").alias("operating_hours"),
        col("after_parsed.is_active").alias("is_active"),
        
        to_timestamp(col("after_parsed.created_at") / 1000).alias("created_at"),
        to_timestamp(col("after_parsed.updated_at") / 1000).alias("updated_at"),
        
        col("payload_parsed.payload.op").alias("cdc_operation"),
        to_timestamp(col("payload_parsed.payload.ts_ms") / 1000).alias("cdc_timestamp"),
        col("payload_parsed.payload.source.snapshot").alias("cdc_snapshot"),
        
        col("kafka_key").alias("kafka_key"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("kafka_timestamp").alias("kafka_timestamp")
    )
    
    return parsed_stream


@dlt.table(
    name="courier_bronze",
    comment="Raw CDC data for couriers from Kafka",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "courier_id"}
)
def courier_bronze():
    """Ingestion streaming depuis Kafka topic ubear.public.courier"""
    
    raw_stream = read_kafka_topic("ubear.public.courier")
    
    parsed_stream = raw_stream.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp"
    ).withColumn(
        "payload_parsed",
        from_json(col("raw_json"), debezium_envelope_schema)
    ).withColumn(
        "after_parsed",
        from_json(col("payload_parsed.payload.after"), courier_payload_schema)
    ).select(
        col("after_parsed.courier_id").alias("courier_id"),
        col("after_parsed.courier_uuid").alias("courier_uuid"),
        col("after_parsed.first_name").alias("first_name"),
        col("after_parsed.last_name").alias("last_name"),
        col("after_parsed.email").alias("email"),
        col("after_parsed.phone_number").alias("phone_number"),
        col("after_parsed.vehicle_type").alias("vehicle_type"),
        col("after_parsed.license_plate").alias("license_plate"),
        col("after_parsed.is_active").alias("is_active"),
        col("after_parsed.onboarding_date").cast("date").alias("onboarding_date"),
        
        to_timestamp(col("after_parsed.created_at") / 1000).alias("created_at"),
        to_timestamp(col("after_parsed.updated_at") / 1000).alias("updated_at"),
        
        col("payload_parsed.payload.op").alias("cdc_operation"),
        to_timestamp(col("payload_parsed.payload.ts_ms") / 1000).alias("cdc_timestamp"),
        col("payload_parsed.payload.source.snapshot").alias("cdc_snapshot"),
        
        col("kafka_key").alias("kafka_key"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("kafka_timestamp").alias("kafka_timestamp")
    )
    
    return parsed_stream


@dlt.table(
    name="trip_events_bronze",
    comment="Raw CDC data for trip events from Kafka",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "event_id,trip_id"}
)
def trip_events_bronze():
    """Ingestion streaming depuis Kafka topic ubear.public.trip_events"""
    
    raw_stream = read_kafka_topic("ubear.public.trip_events")
    
    parsed_stream = raw_stream.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp"
    ).withColumn(
        "payload_parsed",
        from_json(col("raw_json"), debezium_envelope_schema)
    ).withColumn(
        "after_parsed",
        from_json(col("payload_parsed.payload.after"), trip_events_payload_schema)
    ).select(
        col("after_parsed.event_id").alias("event_id"),
        col("after_parsed.trip_id").alias("trip_id"),
        col("after_parsed.event_type").alias("event_type"),
        to_timestamp(col("after_parsed.event_timestamp") / 1000).alias("event_timestamp"),
        col("after_parsed.latitude").cast("decimal(10,7)").alias("latitude"),
        col("after_parsed.longitude").cast("decimal(10,7)").alias("longitude"),
        col("after_parsed.actor_type").alias("actor_type"),
        col("after_parsed.actor_id").alias("actor_id"),
        col("after_parsed.event_details").alias("event_details"),
        
        to_timestamp(col("after_parsed.created_at") / 1000).alias("created_at"),
        
        col("payload_parsed.payload.op").alias("cdc_operation"),
        to_timestamp(col("payload_parsed.payload.ts_ms") / 1000).alias("cdc_timestamp"),
        col("payload_parsed.payload.source.snapshot").alias("cdc_snapshot"),
        
        col("kafka_key").alias("kafka_key"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("kafka_timestamp").alias("kafka_timestamp")
    )
    
    return parsed_stream
