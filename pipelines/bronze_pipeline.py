"""
Bronze Pipeline pour Databricks avec Google Cloud Pub/Sub
Pattern simplifié sans fichier DBFS - Compatible avec Free Trial
Basé sur le pattern Kinesis streaming
"""

import dlt
from pyspark.sql.functions import from_json, col, struct, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType

# =============================================================================
# Configuration Pub/Sub (via pipeline parameters)
# =============================================================================

# Ces valeurs seront passées via les paramètres du pipeline DLT
GCP_PROJECT_ID = spark.conf.get("gcp.project.id", "gentle-voltage-478517-q0")

# Configuration Pub/Sub pour chaque subscription
pubsub_base_config = {
    "projectId": GCP_PROJECT_ID,
    "credentialsJson": spark.conf.get("gcp.credentials.json", "")  # Passé en paramètre
}

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
    StructField("operating_hours", StringType(), True),  # JSON as string
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
    StructField("onboarding_date", StringType(), True),  # Date as string
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True)
])

trip_events_payload_schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("trip_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("eater_id", IntegerType(), True),
    StructField("merchant_id", IntegerType(), True),
    StructField("courier_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),  # Timestamp as string
    StructField("payload", StringType(), True),  # JSONB as string
    StructField("created_at", LongType(), True)
])

# =============================================================================
# Tables Bronze - Pattern Kinesis-like
# =============================================================================

@dlt.table(
    name="eater_bronze",
    comment="Raw CDC data for eaters from Pub/Sub",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "eater_id"}
)
def eater_bronze():
    """Ingestion streaming depuis Pub/Sub subscription ubear-eater-sub"""
    
    # Configuration spécifique pour eater
    config = pubsub_base_config.copy()
    config["subscriptionId"] = "ubear-eater-sub"
    
    # Read stream depuis Pub/Sub
    raw_stream = (
        spark.readStream
        .format("pubsub")
        .options(**config)
        .load()
    )
    
    # Parser le message Debezium CDC
    parsed_stream = raw_stream.selectExpr(
        "CAST(data AS STRING) AS raw_json",
        "messageId",
        "publishTime",
        "attributes"
    ).withColumn(
        # Parser le JSON Debezium envelope
        "payload_parsed",
        from_json(col("raw_json"), StructType([
            StructField("payload", StructType([
                StructField("before", eater_payload_schema, True),
                StructField("after", eater_payload_schema, True),
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
        ]))
    ).withColumn(
        # Metadata Pub/Sub
        "pubsub_metadata",
        struct(
            col("messageId"),
            col("publishTime"),
            col("attributes")
        )
    )
    
    # Sélectionner les colonnes finales (on prend "after" pour INSERT/UPDATE)
    return parsed_stream.select(
        col("payload_parsed.payload.after.*"),
        col("payload_parsed.payload.op").alias("cdc_operation"),
        to_timestamp(col("payload_parsed.payload.ts_ms") / 1000).alias("cdc_timestamp"),
        col("payload_parsed.payload.source.lsn").alias("cdc_lsn"),
        col("pubsub_metadata")
    )


@dlt.table(
    name="merchant_bronze",
    comment="Raw CDC data for merchants from Pub/Sub",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "merchant_id"}
)
def merchant_bronze():
    """Ingestion streaming depuis Pub/Sub subscription ubear-merchant-sub"""
    
    config = pubsub_base_config.copy()
    config["subscriptionId"] = "ubear-merchant-sub"
    
    raw_stream = (
        spark.readStream
        .format("pubsub")
        .options(**config)
        .load()
    )
    
    parsed_stream = raw_stream.selectExpr(
        "CAST(data AS STRING) AS raw_json",
        "messageId",
        "publishTime",
        "attributes"
    ).withColumn(
        "payload_parsed",
        from_json(col("raw_json"), StructType([
            StructField("payload", StructType([
                StructField("before", merchant_payload_schema, True),
                StructField("after", merchant_payload_schema, True),
                StructField("source", StructType([
                    StructField("version", StringType(), True),
                    StructField("ts_ms", LongType(), True),
                    StructField("snapshot", StringType(), True),
                    StructField("db", StringType(), True),
                    StructField("table", StringType(), True)
                ]), True),
                StructField("op", StringType(), True),
                StructField("ts_ms", LongType(), True)
            ]), True)
        ]))
    ).withColumn(
        "pubsub_metadata",
        struct(
            col("messageId"),
            col("publishTime"),
            col("attributes")
        )
    )
    
    return parsed_stream.select(
        col("payload_parsed.payload.after.*"),
        col("payload_parsed.payload.op").alias("cdc_operation"),
        to_timestamp(col("payload_parsed.payload.ts_ms") / 1000).alias("cdc_timestamp"),
        col("pubsub_metadata")
    )


@dlt.table(
    name="courier_bronze",
    comment="Raw CDC data for couriers from Pub/Sub",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "courier_id"}
)
def courier_bronze():
    """Ingestion streaming depuis Pub/Sub subscription ubear-courier-sub"""
    
    config = pubsub_base_config.copy()
    config["subscriptionId"] = "ubear-courier-sub"
    
    raw_stream = (
        spark.readStream
        .format("pubsub")
        .options(**config)
        .load()
    )
    
    parsed_stream = raw_stream.selectExpr(
        "CAST(data AS STRING) AS raw_json",
        "messageId",
        "publishTime",
        "attributes"
    ).withColumn(
        "payload_parsed",
        from_json(col("raw_json"), StructType([
            StructField("payload", StructType([
                StructField("before", courier_payload_schema, True),
                StructField("after", courier_payload_schema, True),
                StructField("source", StructType([
                    StructField("ts_ms", LongType(), True),
                    StructField("table", StringType(), True)
                ]), True),
                StructField("op", StringType(), True),
                StructField("ts_ms", LongType(), True)
            ]), True)
        ]))
    ).withColumn(
        "pubsub_metadata",
        struct(
            col("messageId"),
            col("publishTime"),
            col("attributes")
        )
    )
    
    return parsed_stream.select(
        col("payload_parsed.payload.after.*"),
        col("payload_parsed.payload.op").alias("cdc_operation"),
        to_timestamp(col("payload_parsed.payload.ts_ms") / 1000).alias("cdc_timestamp"),
        col("pubsub_metadata")
    )


@dlt.table(
    name="trip_events_bronze",
    comment="Raw CDC data for trip events from Pub/Sub",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "trip_id,event_id"}
)
def trip_events_bronze():
    """Ingestion streaming depuis Pub/Sub subscription ubear-trip-events-sub"""
    
    config = pubsub_base_config.copy()
    config["subscriptionId"] = "ubear-trip-events-sub"
    
    raw_stream = (
        spark.readStream
        .format("pubsub")
        .options(**config)
        .load()
    )
    
    parsed_stream = raw_stream.selectExpr(
        "CAST(data AS STRING) AS raw_json",
        "messageId",
        "publishTime",
        "attributes"
    ).withColumn(
        "payload_parsed",
        from_json(col("raw_json"), StructType([
            StructField("payload", StructType([
                StructField("before", trip_events_payload_schema, True),
                StructField("after", trip_events_payload_schema, True),
                StructField("source", StructType([
                    StructField("ts_ms", LongType(), True),
                    StructField("table", StringType(), True)
                ]), True),
                StructField("op", StringType(), True),
                StructField("ts_ms", LongType(), True)
            ]), True)
        ]))
    ).withColumn(
        "pubsub_metadata",
        struct(
            col("messageId"),
            col("publishTime"),
            col("attributes")
        )
    )
    
    return parsed_stream.select(
        col("payload_parsed.payload.after.*"),
        col("payload_parsed.payload.op").alias("cdc_operation"),
        to_timestamp(col("payload_parsed.payload.ts_ms") / 1000).alias("cdc_timestamp"),
        col("pubsub_metadata")
    )
