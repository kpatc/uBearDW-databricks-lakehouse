# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Pipeline - Transformation et Enrichissement
# MAGIC
# MAGIC **Pipeline Delta Live Tables pour le layer Silver**
# MAGIC 
# MAGIC Ce pipeline transforme les données Bronze en données Silver nettoyées et enrichies.
# MAGIC - Parse le payload JSONB des trip_events
# MAGIC - Applique règles de qualité et validation
# MAGIC - Normalise et nettoie les données
# MAGIC - Calcule des métriques dérivées
# MAGIC
# MAGIC ## Sources
# MAGIC - Tables Bronze (trip_events_bronze, eater_bronze, merchant_bronze, courier_bronze)
# MAGIC
# MAGIC ## Tables créées
# MAGIC - `trip_events_silver` - Événements nettoyés avec payload parsé
# MAGIC - `eater_silver` - Clients normalisés
# MAGIC - `merchant_silver` - Restaurants normalisés
# MAGIC - `courier_silver` - Livreurs normalisés

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports et Configuration

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, from_json, to_date, trim, upper, lower,
    regexp_replace, current_timestamp, coalesce, lit,
    when, concat_ws, datediff, round as spark_round,
    row_number, count, sum as spark_sum, avg, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    TimestampType, DecimalType, BooleanType, DoubleType, ArrayType
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schéma du Payload Trip Events (JSONB)

# COMMAND ----------

# Schéma du payload JSON stocké dans PostgreSQL
# Contient tous les détails de la commande
trip_payload_schema = StructType([
    StructField("subtotal_amount", DoubleType(), True),
    StructField("delivery_fee", DoubleType(), True),
    StructField("service_fee", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("courier_payout", DoubleType(), True),
    StructField("distance_miles", DoubleType(), True),
    StructField("total_distance_miles", DoubleType(), True),
    StructField("courier_distance_miles", DoubleType(), True),
    StructField("preparation_time_minutes", IntegerType(), True),
    StructField("estimated_prep_time_minutes", IntegerType(), True),
    StructField("actual_prep_time_minutes", IntegerType(), True),
    StructField("delivery_time_minutes", IntegerType(), True),
    StructField("total_time_minutes", IntegerType(), True),
    StructField("trip_status", StringType(), True),
    StructField("is_group_order", BooleanType(), True),
    StructField("promo_code", StringType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("eater_rating", IntegerType(), True),
    StructField("courier_rating", IntegerType(), True),
    StructField("merchant_rating", IntegerType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("items", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True)
    ])), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Silver: trip_events_silver

# COMMAND ----------

@dlt.table(
    name="trip_events_silver",
    comment="Table silver streaming - Trip events enrichis avec payload parsé et metrics calculées",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "trip_id,event_time",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_trip_id", "trip_id IS NOT NULL")
@dlt.expect("valid_event_type", "event_type IS NOT NULL")
def trip_events_silver():
    """
    Transformation Bronze -> Silver pour trip_events.
    - Trip lifecycle events avec order, eater, merchant, courier tracking
    - Les données sont déjà parsées dans Bronze
    - Applique nettoyage et validation
    """
    df = dlt.read_stream("ubear_catalog.ubear_bronze.trip_events_bronze")
    
    # Les colonnes sont déjà extraites du payload dans Bronze
    silver_df = df.select(
        col("event_id"),
        col("trip_id"),
        col("order_id"),
        col("eater_id"),
        col("merchant_id"),
        col("courier_id"),
        col("event_type"),
        col("event_time"),
        col("payload"),
        col("cdc_operation"),
        col("cdc_timestamp"),
        col("cdc_snapshot"),
        col("created_at"),
        col("kafka_timestamp"),
        current_timestamp().alias("silver_load_time")
    )
    
    return silver_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Silver: eater_silver (avec métriques enrichies)

# COMMAND ----------

@dlt.table(
    name="eater_silver",
    comment="Table silver streaming - Clients normalisés (ready for SCD2 in Gold)",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_eater_id", "eater_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
@dlt.expect("valid_email_format", "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Z|a-z]{2,}$'")
def eater_silver():
    """
    Transformation Bronze -> Silver pour eaters.
    - Normalise email (lowercase)
    - Nettoie adresses et téléphones
    - Prépare pour enrichissement Gold (lifetime metrics calculées dans Gold)
    """
    df = dlt.read_stream("ubear_catalog.ubear_bronze.eater_bronze")
    
    cleaned_df = df.select(
        col("eater_id"),
        col("eater_uuid"),
        # Noms normalisés
        trim(col("first_name")).alias("first_name"),
        trim(col("last_name")).alias("last_name"),
        # Email en lowercase pour consistency
        lower(trim(col("email"))).alias("email"),
        # Téléphone nettoyé
        regexp_replace(col("phone_number"), "[^0-9+]", "").alias("phone_number"),
        # Adresses normalisées
        trim(col("address_line_1")).alias("address_line_1"),
        trim(col("address_line_2")).alias("address_line_2"),
        trim(col("city")).alias("city"),
        trim(col("state_province")).alias("state_province"),
        regexp_replace(col("postal_code"), "[^0-9A-Z]", "").alias("postal_code"),
        upper(trim(col("country"))).alias("country"),
        # Autres champs
        col("default_payment_method"),
        col("is_active"),
        col("created_at"),
        col("updated_at"),
        col("kafka_timestamp"),
        current_timestamp().alias("silver_load_time")
    ).dropDuplicates(["eater_id"])
    
    return cleaned_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Silver: merchant_silver

# COMMAND ----------

@dlt.table(
    name="merchant_silver",
    comment="Table silver streaming - Restaurants normalisés (ready for SCD2 in Gold)",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dlt.expect("valid_merchant_name", "name IS NOT NULL")
@dlt.expect("valid_city", "city IS NOT NULL")
def merchant_silver():
    """
    Transformation Bronze -> Silver pour merchants.
    - Normalise noms et adresses
    - Standardise types de cuisine
    - Prépare pour enrichissement Gold (ratings, metrics calculées dans Gold)
    """
    df = dlt.read_stream("ubear_catalog.ubear_bronze.merchant_bronze")
    
    cleaned_df = df.select(
        col("merchant_id"),
        col("merchant_uuid"),
        # Nom nettoyé
        trim(col("name")).alias("name"),
        # Contact
        lower(trim(col("email"))).alias("email"),
        regexp_replace(col("phone_number"), "[^0-9+]", "").alias("phone_number"),
        # Type et cuisine
        trim(col("business_type")).alias("business_type"),
        trim(lower(col("cuisine_type"))).alias("cuisine_type"),
        # Adresses normalisées
        trim(col("address_line_1")).alias("address_line_1"),
        trim(col("address_line_2")).alias("address_line_2"),
        trim(col("city")).alias("city"),
        trim(col("state_province")).alias("state_province"),
        regexp_replace(col("postal_code"), "[^0-9A-Z]", "").alias("postal_code"),
        upper(trim(col("country"))).alias("country"),
        # Autres
        col("operating_hours"),
        col("is_active"),
        col("created_at"),
        col("updated_at"),
        col("kafka_timestamp"),
        current_timestamp().alias("silver_load_time")
    ).dropDuplicates(["merchant_id"])
    
    return cleaned_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Silver: courier_silver

# COMMAND ----------

@dlt.table(
    name="courier_silver",
    comment="Table silver streaming - Livreurs normalisés (ready for SCD2 in Gold)",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_courier_id", "courier_id IS NOT NULL")
@dlt.expect("valid_courier_name", "first_name IS NOT NULL AND last_name IS NOT NULL")
@dlt.expect("valid_vehicle", "vehicle_type IS NOT NULL")
def courier_silver():
    """
    Transformation Bronze -> Silver pour couriers.
    - Normalise noms et véhicules
    - Standardise license_plate
    - Prépare pour enrichissement Gold (ratings, earnings calculés dans Gold)
    """
    df = dlt.read_stream("ubear_catalog.ubear_bronze.courier_bronze")
    
    cleaned_df = df.select(
        col("courier_id"),
        col("courier_uuid"),
        # Noms
        trim(col("first_name")).alias("first_name"),
        trim(col("last_name")).alias("last_name"),
        # Contact
        lower(trim(col("email"))).alias("email"),
        regexp_replace(col("phone_number"), "[^0-9+]", "").alias("phone_number"),
        # Véhicule (normalisé: scooter, bike, car, walking)
        lower(trim(col("vehicle_type"))).alias("vehicle_type"),
        upper(regexp_replace(col("license_plate"), "[^A-Z0-9-]", "")).alias("license_plate"),
        # Status et dates
        col("is_active"),
        col("onboarding_date"),
        col("created_at"),
        col("updated_at"),
        col("kafka_timestamp"),
        current_timestamp().alias("silver_load_time")
    ).dropDuplicates(["courier_id"])
    
    return cleaned_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Pipeline
# MAGIC
# MAGIC Pour créer ce pipeline dans Databricks:
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "ubear_silver_streaming",
# MAGIC   "storage": "/mnt/datalake/ubear/dlt/silver",
# MAGIC   "target": "ubear_silver",
# MAGIC   "notebooks": ["/Workspace/Repos/ubear-dw/pipelines/silver_pipeline"],
# MAGIC   "continuous": true,
# MAGIC   "channel": "CURRENT"
# MAGIC }
# MAGIC ```
