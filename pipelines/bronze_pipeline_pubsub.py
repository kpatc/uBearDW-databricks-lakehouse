"""
Bronze Pipeline pour Databricks avec Google Cloud Pub/Sub
Ingestion CDC depuis Pub/Sub au lieu de Kafka
Pattern similaire à Kinesis pour éviter les problèmes de DBFS avec free trial
"""

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =============================================================================
# Configuration Pub/Sub
# =============================================================================

# Configuration via pipeline parameters (pas de fichier nécessaire)
GCP_PROJECT_ID = spark.conf.get("gcp.project.id", "gentle-voltage-478517-q0")
GCP_CREDENTIALS = spark.conf.get("gcp.credentials.json", "")  # JSON string inline

# Subscriptions Pub/Sub
EATER_SUBSCRIPTION = "projects/{}/subscriptions/ubear-eater-sub".format(GCP_PROJECT_ID)
MERCHANT_SUBSCRIPTION = "projects/{}/subscriptions/ubear-merchant-sub".format(GCP_PROJECT_ID)
COURIER_SUBSCRIPTION = "projects/{}/subscriptions/ubear-courier-sub".format(GCP_PROJECT_ID)
TRIP_EVENTS_SUBSCRIPTION = "projects/{}/subscriptions/ubear-trip-events-sub".format(GCP_PROJECT_ID)

# =============================================================================
# Schemas
# =============================================================================

eater_schema = StructType([
    StructField("eater_id", IntegerType(), False),
    StructField("eater_uuid", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
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

merchant_schema = StructType([
    StructField("merchant_id", IntegerType(), False),
    StructField("merchant_uuid", StringType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone_number", StringType(), True),
    StructField("business_type", StringType(), True),
    StructField("cuisine_type", StringType(), True),
    StructField("address_line_1", StringType(), True),
    StructField("address_line_2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("operating_hours", StringType(), True),  # JSONB as string
    StructField("is_active", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

courier_schema = StructType([
    StructField("courier_id", IntegerType(), False),
    StructField("courier_uuid", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone_number", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("license_plate", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("onboarding_date", DateType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

trip_events_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("trip_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("eater_id", IntegerType(), True),
    StructField("merchant_id", IntegerType(), True),
    StructField("courier_id", IntegerType(), True),
    StructField("event_type", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("payload", StringType(), True),  # JSONB as string
    StructField("created_at", TimestampType(), True)
])

# =============================================================================
# Bronze Tables avec Pub/Sub
# =============================================================================

@dlt.table(
    name="eater_bronze",
    comment="Raw eater data from Cloud SQL via Pub/Sub CDC",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "eater_id"
    }
)
def eater_bronze():
    """Ingestion eater depuis Pub/Sub"""
    return (
        spark.readStream
            .format("pubsub")
            .option("subscriptionId", EATER_SUBSCRIPTION)
            .option("credentialsFile", GCP_SERVICE_ACCOUNT_JSON)
            .load()
            .select(
                from_json(col("data").cast("string"), eater_schema).alias("after")
            )
            .select("after.*")
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source", lit("pubsub_cdc"))
    )

@dlt.table(
    name="merchant_bronze",
    comment="Raw merchant data from Cloud SQL via Pub/Sub CDC",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "merchant_id"
    }
)
def merchant_bronze():
    """Ingestion merchant depuis Pub/Sub"""
    return (
        spark.readStream
            .format("pubsub")
            .option("subscriptionId", MERCHANT_SUBSCRIPTION)
            .option("credentialsFile", GCP_SERVICE_ACCOUNT_JSON)
            .load()
            .select(
                from_json(col("data").cast("string"), merchant_schema).alias("after")
            )
            .select("after.*")
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source", lit("pubsub_cdc"))
    )

@dlt.table(
    name="courier_bronze",
    comment="Raw courier data from Cloud SQL via Pub/Sub CDC",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "courier_id"
    }
)
def courier_bronze():
    """Ingestion courier depuis Pub/Sub"""
    return (
        spark.readStream
            .format("pubsub")
            .option("subscriptionId", COURIER_SUBSCRIPTION)
            .option("credentialsFile", GCP_SERVICE_ACCOUNT_JSON)
            .load()
            .select(
                from_json(col("data").cast("string"), courier_schema).alias("after")
            )
            .select("after.*")
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source", lit("pubsub_cdc"))
    )

@dlt.table(
    name="trip_events_bronze",
    comment="Raw trip events from Cloud SQL via Pub/Sub CDC",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "trip_id,event_time"
    }
)
def trip_events_bronze():
    """Ingestion trip_events depuis Pub/Sub"""
    return (
        spark.readStream
            .format("pubsub")
            .option("subscriptionId", TRIP_EVENTS_SUBSCRIPTION)
            .option("credentialsFile", GCP_SERVICE_ACCOUNT_JSON)
            .load()
            .select(
                from_json(col("data").cast("string"), trip_events_schema).alias("after")
            )
            .select("after.*")
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source", lit("pubsub_cdc"))
            .withWatermark("event_time", "10 minutes")
    )
