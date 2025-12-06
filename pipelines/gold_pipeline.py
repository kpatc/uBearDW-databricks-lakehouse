# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Pipeline - Data Warehouse Layer avec Enrichissements
# MAGIC
# MAGIC **Pipeline Batch pour le layer Gold (Data Warehouse final)**
# MAGIC 
# MAGIC Ce notebook implémente:
# MAGIC - **Dimensions SCD Type 2** enrichies avec métriques agrégées
# MAGIC - **dim_location** avec geocoding et indexation spatiale (geohash, H3)
# MAGIC - **Dimensions statiques** (dim_date, dim_time)
# MAGIC - **trip_fact** avec toutes les métriques business
# MAGIC
# MAGIC **Exécution:** Batch quotidien via Databricks Job (2 AM UTC)
# MAGIC
# MAGIC ## Architecture d'enrichissement
# MAGIC ```
# MAGIC Silver (données nettoyées) 
# MAGIC   ↓
# MAGIC Gold Enrichissement:
# MAGIC   - Calcul métriques agrégées (lifetime orders, avg ratings, etc.)
# MAGIC   - Geocoding API (adresse → lat/lon)
# MAGIC   - Calcul geohash & H3 index
# MAGIC   - Assignation timezone, neighborhood
# MAGIC   - SCD2 pour tracer l'historique
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration et Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col, sha2, concat_ws, current_timestamp, lit, 
    when, coalesce, row_number, max as spark_max, min as spark_min,
    sum as spark_sum, avg, count, countDistinct, round as spark_round,
    datediff, date_format, dayofweek, weekofyear, month, quarter, year,
    expr, concat, lpad, to_date, hour, minute, from_unixtime,
    array_agg, collect_list, first, last, lag, lead, dense_rank
)
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from typing import List, Dict
import geohash2 as geohash  # Pour calcul geohash
import h3  # Pour calcul H3 index

# Configuration
CATALOG = "ubear_catalog"
SCHEMA_SILVER = "ubear_silver"
SCHEMA_GOLD = "ubear_gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fonctions UDF pour Enrichissement Géographique

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# UDF pour calculer geohash depuis lat/lon
@udf(returnType=StringType())
def calculate_geohash(lat: float, lon: float, precision: int = 8) -> str:
    """Calcule le geohash à partir de latitude/longitude"""
    if lat is None or lon is None:
        return None
    try:
        return geohash.encode(lat, lon, precision=precision)
    except:
        return None

# UDF pour calculer H3 index
@udf(returnType=StringType())
def calculate_h3_index(lat: float, lon: float, resolution: int = 8) -> str:
    """Calcule le H3 index à partir de latitude/longitude"""
    if lat is None or lon is None:
        return None
    try:
        return h3.geo_to_h3(lat, lon, resolution)
    except:
        return None

# UDF pour déterminer timezone depuis lat/lon (simplifié)
@udf(returnType=StringType())
def get_timezone_from_coords(lat: float, lon: float) -> str:
    """Retourne timezone approximatif basé sur coordonnées (simplifié)"""
    if lat is None or lon is None:
        return "UTC"
    # Logique simplifiée - En production, utiliser timezonefinder library
    if 41 <= lat <= 51 and -5 <= lon <= 10:
        return "Europe/Paris"
    elif 48 <= lat <= 51 and -2 <= lon <= 2:
        return "Europe/London"
    else:
        return "UTC"

# Enregistrer les UDFs
spark.udf.register("calculate_geohash", calculate_geohash)
spark.udf.register("calculate_h3_index", calculate_h3_index)
spark.udf.register("get_timezone_from_coords", get_timezone_from_coords)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fonction SCD Type 2 Générique

# COMMAND ----------

def apply_scd2_merge(
    source_df: DataFrame,
    target_table: str,
    business_keys: List[str],
    compare_columns: List[str],
    additional_columns: Dict[str, any] = None
) -> None:
    """
    Applique la logique SCD Type 2 avec MERGE Delta Lake.
    
    Args:
        source_df: DataFrame source (données Silver enrichies)
        target_table: Nom complet de la table cible (catalog.schema.table)
        business_keys: Liste des clés métier (ex: ['eater_id'])
        compare_columns: Colonnes à comparer pour détecter les changements
        additional_columns: Colonnes supplémentaires à ajouter
    """
    
    # Calculer hash pour détecter changements
    source_df = source_df.withColumn(
        "row_hash",
        sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in compare_columns]), 256)
    )
    
    # Vérifier si la table existe
    table_exists = False
    try:
        table_exists = spark.catalog.tableExists(target_table)
    except:
        table_exists = False
    
    if not table_exists:
        # Première charge - insérer toutes les lignes avec colonnes SCD2
        initial_df = (source_df
            .withColumn("effective_start_date", current_timestamp())
            .withColumn("effective_end_date", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
            .withColumn("version_number", lit(1))
        )
        
        if additional_columns:
            for col_name, col_value in additional_columns.items():
                initial_df = initial_df.withColumn(col_name, col_value)
        
        initial_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        print(f"✅ {target_table} créée avec {initial_df.count():,} lignes")
        return
    
    # Récupérer les enregistrements courants
    target_delta = DeltaTable.forName(spark, target_table)
    current_records = spark.table(target_table).filter(col("is_current") == True)
    
    # Joindre source et cible sur business keys
    joined = source_df.alias("source").join(
        current_records.alias("target"),
        on=business_keys,
        how="left"
    )
    
    # Identifier les changements (row_hash différent)
    changes = joined.filter(
        (col("target.row_hash").isNull()) | 
        (col("source.row_hash") != col("target.row_hash"))
    )
    
    changes_count = changes.count()
    if changes_count == 0:
        print(f"ℹ️  Aucun changement détecté pour {target_table}")
        return
    
    print(f"🔄 {changes_count:,} changements détectés")
    
    # Étape 1: Expirer les anciens enregistrements
    expired_keys = changes.select(*[col(f"source.{k}").alias(k) for k in business_keys]).distinct()
    
    target_delta.alias("target").merge(
        expired_keys.alias("updates"),
        " AND ".join([f"target.{k} = updates.{k}" for k in business_keys]) + " AND target.is_current = true"
    ).whenMatchedUpdate(
        set={
            "is_current": lit(False),
            "effective_end_date": current_timestamp()
        }
    ).execute()
    
    # Étape 2: Calculer version suivante
    max_versions = (
        spark.table(target_table)
        .groupBy(*business_keys)
        .agg(spark_max("version_number").alias("max_version"))
    )
    
    new_records = changes.select("source.*").join(
        max_versions,
        on=business_keys,
        how="left"
    ).withColumn(
        "version_number",
        coalesce(col("max_version"), lit(0)) + 1
    ).drop("max_version")
    
    # Étape 3: Ajouter colonnes SCD2
    new_records = (new_records
        .withColumn("effective_start_date", current_timestamp())
        .withColumn("effective_end_date", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
    )
    
    if additional_columns:
        for col_name, col_value in additional_columns.items():
            new_records = new_records.withColumn(col_name, col_value)
    
    # Étape 4: Insérer les nouveaux enregistrements
    new_records.write.format("delta").mode("append").saveAsTable(target_table)
    
    print(f"✅ {new_records.count():,} nouveaux enregistrements insérés dans {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_location (Enrichissement Géographique)

# COMMAND ----------

def build_dim_location():
    """
    Construit dim_location en extrayant toutes les adresses uniques
    depuis eater et merchant, puis enrichit avec:
    - Geocoding (lat/lon) - Simulé avec coordonnées factices
    - Geohash calculation
    - H3 index calculation
    - Timezone assignment
    - Neighborhood/region mapping
    """
    
    # Extraire adresses des eaters
    eater_addresses = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.eater_silver").select(
        col("address_line_1"),
        col("address_line_2"),
        col("city"),
        col("state_province"),
        col("postal_code"),
        col("country"),
        lit("dropoff").alias("location_type")
    )
    
    # Extraire adresses des merchants
    merchant_addresses = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.merchant_silver").select(
        col("address_line_1"),
        col("address_line_2"),
        col("city"),
        col("state_province"),
        col("postal_code"),
        col("country"),
        lit("pickup").alias("location_type")
    )
    
    # Union et dédupliquer
    all_addresses = eater_addresses.union(merchant_addresses).dropDuplicates([
        "address_line_1", "city", "postal_code"
    ])
    
    # ⚠️ GEOCODING SIMULÉ - En production, utiliser Google Maps API / Azure Maps
    # Ici on génère des coordonnées factices basées sur la ville
    locations_with_coords = all_addresses.withColumn(
        "latitude",
        when(col("city") == "Paris", 48.8566 + (expr("rand()") * 0.1 - 0.05))
        .when(col("city") == "Lyon", 45.7640 + (expr("rand()") * 0.1 - 0.05))
        .when(col("city") == "Marseille", 43.2965 + (expr("rand()") * 0.1 - 0.05))
        .otherwise(48.8566)
    ).withColumn(
        "longitude",
        when(col("city") == "Paris", 2.3522 + (expr("rand()") * 0.1 - 0.05))
        .when(col("city") == "Lyon", 4.8357 + (expr("rand()") * 0.1 - 0.05))
        .when(col("city") == "Marseille", 5.3698 + (expr("rand()") * 0.1 - 0.05))
        .otherwise(2.3522)
    )
    
    # Calcul geohash et H3 index
    enriched_locations = (locations_with_coords
        .withColumn("geohash", calculate_geohash(col("latitude"), col("longitude"), lit(8)))
        .withColumn("h3_index", calculate_h3_index(col("latitude"), col("longitude"), lit(8)))
        .withColumn("timezone", get_timezone_from_coords(col("latitude"), col("longitude")))
    )
    
    # Assignation neighborhood et region_zone (basé sur city)
    final_locations = (enriched_locations
        .withColumn("neighborhood", 
            when(col("city") == "Paris", 
                when(col("postal_code").startswith("75001"), "Louvre")
                .when(col("postal_code").startswith("75002"), "Bourse")
                .when(col("postal_code").startswith("75004"), "Marais")
                .when(col("postal_code").startswith("75008"), "Champs-Elysees")
                .when(col("postal_code").startswith("75009"), "Opera")
                .otherwise("Centre"))
            .when(col("city") == "Lyon", "Centre-Ville")
            .when(col("city") == "Marseille", "Vieux-Port")
            .otherwise(col("city"))
        )
        .withColumn("region_zone",
            when(col("state_province") == "Ile-de-France", "paris-region")
            .when(col("state_province") == "Auvergne-Rhone-Alpes", "lyon-region")
            .when(col("state_province") == "Provence-Alpes-Cote", "marseille-region")
            .otherwise("other")
        )
        .withColumn("is_high_rise", lit(False))  # TODO: enrichir avec données OSM
        .withColumn("has_doorman", lit(False))
        .withColumn("special_instructions", lit(None).cast("string"))
    )
    
    # Ajouter location_id auto-incrémenté
    window_spec = Window.orderBy("address_line_1", "city")
    dim_location = (final_locations
        .withColumn("location_id", row_number().over(window_spec))
        .select(
            "location_id", "address_line_1", "address_line_2", "city",
            "state_province", "postal_code", "country",
            "latitude", "longitude", "geohash", "h3_index",
            "neighborhood", "region_zone", "location_type",
            "is_high_rise", "has_doorman", "special_instructions", "timezone"
        )
    )
    
    # Écrire dans Gold
    dim_location.write.format("delta") \
        .partitionBy("region_zone") \
        .mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_location")
    
    print(f"✅ dim_location créée avec {dim_location.count():,} locations uniques")

build_dim_location()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_eater (SCD2 + Métriques Enrichies)

# COMMAND ----------

# Lire données Silver
eater_silver_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.eater_silver")
trip_events_silver_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.trip_events_silver")

# Calculer métriques agrégées par eater depuis trip_events
# Note: trip_status n'existe pas, on prend tous les events
eater_metrics = (trip_events_silver_df
    # .filter(col("trip_status") == "delivered")
    .groupBy("eater_id")
    .agg(
        count("*").alias("total_lifetime_orders"),
        spark_sum("total_amount").alias("total_lifetime_spend"),
        avg("total_amount").alias("average_order_value"),
        # Cuisine préférée (most frequent)
        first(col("merchant_id")).alias("favorite_merchant_id"),  # Simplification
        avg("eater_rating").alias("avg_rating_given"),
        countDistinct("order_id").alias("distinct_orders")
    )
    .withColumn("total_lifetime_spend", spark_round(col("total_lifetime_spend"), 2))
    .withColumn("average_order_value", spark_round(col("average_order_value"), 2))
)

# Calculer loyalty tier basé sur lifetime orders
eater_with_metrics = eater_silver_df.join(eater_metrics, "eater_id", "left").withColumn(
    "loyalty_tier",
    when(col("total_lifetime_orders") >= 50, "platinum")
    .when(col("total_lifetime_orders") >= 25, "gold")
    .when(col("total_lifetime_orders") >= 10, "silver")
    .otherwise("bronze")
).withColumn(
    "customer_segment",
    when(col("average_order_value") >= 50, "premium")
    .when(col("average_order_value") >= 30, "regular")
    .otherwise("occasional")
).withColumn(
    "is_eats_pass_member", lit(False)  # TODO: enrichir depuis table subscriptions
).withColumn(
    "preferred_cuisine", lit("French")  # TODO: calculer depuis orders réels
).withColumn(
    "dietary_preferences", lit(None).cast("string")
).withColumn(
    "phone_number", col("phone_number")
).withColumn(
    "account_created_date", to_date(col("created_at"))
)

# Colonnes pour SCD2 comparison
compare_cols_eater = [
    'first_name', 'last_name', 'email', 'phone_number',
    'address_line_1', 'address_line_2', 'city', 
    'state_province', 'postal_code', 'country',
    'default_payment_method'
]

# Appliquer SCD2
apply_scd2_merge(
    source_df=eater_with_metrics,
    target_table=f"{CATALOG}.{SCHEMA_GOLD}.dim_eater",
    business_keys=['eater_id'],
    compare_columns=compare_cols_eater
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_merchant (SCD2 + Métriques Enrichies)

# COMMAND ----------

merchant_silver_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.merchant_silver")

# Calculer métriques agrégées depuis trip_events
# Note: Silver ne filtre pas par trip_status, on prend tous les events
merchant_metrics = (trip_events_silver_df
    .groupBy("merchant_id")
    .agg(
        avg("merchant_rating").alias("overall_rating"),
        count(when(col("merchant_rating").isNotNull(), 1)).alias("total_ratings_count"),
        avg("preparation_time_minutes").alias("average_preparation_minutes"),
        count("*").alias("total_orders_completed")
    )
    .withColumn("overall_rating", spark_round(col("overall_rating"), 2))
    .withColumn("average_preparation_minutes", spark_round(col("average_preparation_minutes"), 0).cast("int"))
)

# Enrichir merchant
merchant_with_metrics = merchant_silver_df.join(merchant_metrics, "merchant_id", "left").withColumn(
    "price_range",
    when(col("cuisine_type").isin("french", "japanese"), "$$$")
    .when(col("cuisine_type") == "italian", "$$")
    .otherwise("$$")
).withColumn(
    "merchant_tier",
    when(col("overall_rating") >= 4.5, "premium")
    .when(col("overall_rating") >= 4.0, "standard")
    .otherwise("basic")
).withColumn(
    "is_partner_merchant", lit(True)
).withColumn(
    "commission_rate", lit(18.00)
).withColumn(
    "merchant_onboarding_date", to_date(col("created_at"))
).withColumn(
    "accepts_cash", lit(False)
).withColumn(
    "menu_item_count", lit(35)  # TODO: calculer depuis menu table
).withColumn(
    "average_item_price", lit(15.50)  # TODO: calculer depuis menu items
).withColumn(
    "is_currently_active", col("is_active")
).withColumn(
    "cuisine_subtypes", lit(None).cast("string")
)

compare_cols_merchant = [
    'name', 'email', 'phone_number', 'business_type', 'cuisine_type',
    'address_line_1', 'address_line_2', 'city', 
    'state_province', 'postal_code', 'country',
    'operating_hours', 'is_currently_active'
]

apply_scd2_merge(
    source_df=merchant_with_metrics,
    target_table=f"{CATALOG}.{SCHEMA_GOLD}.dim_merchant",
    business_keys=['merchant_id'],
    compare_columns=compare_cols_merchant
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_courier (SCD2 + Métriques Enrichies)

# COMMAND ----------

courier_silver_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.courier_silver")

# Calculer métriques agrégées
courier_metrics = (trip_events_silver_df
    .groupBy("courier_id")
    .agg(
        count("*").alias("total_deliveries_completed"),
        avg("courier_rating").alias("overall_rating"),
        avg("delivery_time_minutes").alias("average_delivery_time_minutes"),
        spark_sum("total_amount").alias("total_lifetime_earnings"),
        # Calcul on-time rate (simplifié)
        (count(when(col("delivery_time_minutes") <= 30, 1)) / count("*") * 100).alias("on_time_delivery_rate"),
        (count("*") / count("*") * 100).alias("acceptance_rate")  # Simplifié
    )
    .withColumn("overall_rating", spark_round(col("overall_rating"), 2))
    .withColumn("average_delivery_time_minutes", spark_round(col("average_delivery_time_minutes"), 2))
    .withColumn("total_lifetime_earnings", spark_round(col("total_lifetime_earnings"), 2))
    .withColumn("on_time_delivery_rate", spark_round(col("on_time_delivery_rate"), 2))
    .withColumn("acceptance_rate", spark_round(col("acceptance_rate"), 2))
)

courier_with_metrics = courier_silver_df.join(courier_metrics, "courier_id", "left").withColumn(
    "courier_tier",
    when(col("overall_rating") >= 4.8, "platinum")
    .when(col("overall_rating") >= 4.5, "gold")
    .when(col("overall_rating") >= 4.0, "silver")
    .otherwise("bronze")
).withColumn(
    "preferred_delivery_hours", lit("11:00-14:00,18:00-22:00")
).withColumn(
    "has_insulated_bag", lit(True)
).withColumn(
    "background_check_date", col("onboarding_date")
).withColumn(
    "address_line_1", lit(None).cast("string")
).withColumn(
    "address_line_2", lit(None).cast("string")
).withColumn(
    "city", lit(None).cast("string")
).withColumn(
    "state_province", lit(None).cast("string")
).withColumn(
    "postal_code", lit(None).cast("string")
).withColumn(
    "country", lit(None).cast("string")
).withColumn(
    "merchant_tier", lit(None).cast("string")  # Colonne erronée dans schéma original
)

compare_cols_courier = [
    'first_name', 'last_name', 'email', 'phone_number',
    'vehicle_type', 'license_plate', 'is_active'
]

apply_scd2_merge(
    source_df=courier_with_metrics,
    target_table=f"{CATALOG}.{SCHEMA_GOLD}.dim_courier",
    business_keys=['courier_id'],
    compare_columns=compare_cols_courier
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_date (Statique)

# COMMAND ----------

def generate_dim_date(start_date: str = "2020-01-01", end_date: str = "2030-12-31"):
    """Génère la dimension date complète"""
    
    date_df = spark.sql(f"""
        SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_array
    """).selectExpr("explode(date_array) as full_date")
    
    dim_date = (date_df
        .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("day_of_week", dayofweek(col("full_date")))
        .withColumn("day_name", date_format(col("full_date"), "EEEE"))
        .withColumn("is_weekend", col("day_of_week").isin([1, 7]))
        .withColumn("week_of_year", weekofyear(col("full_date")))
        .withColumn("month_number", month(col("full_date")))
        .withColumn("month_name", date_format(col("full_date"), "MMMM"))
        .withColumn("quarter", quarter(col("full_date")))
        .withColumn("year", year(col("full_date")))
        .withColumn("fiscal_year", year(col("full_date")))
        .withColumn("fiscal_quarter", quarter(col("full_date")))
        .withColumn("is_holiday", lit(False))
        .withColumn("holiday_name", lit(None).cast("string"))
    )
    
    return dim_date

dim_date_df = generate_dim_date()
dim_date_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_date")
print(f"✅ dim_date créée avec {dim_date_df.count():,} jours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_time (Statique)

# COMMAND ----------

def generate_dim_time():
    """Génère la dimension temps (minutes de la journée)"""
    
    time_df = spark.sql("SELECT sequence(0, 1439) as minute_of_day").selectExpr("explode(minute_of_day) as minute")
    
    dim_time = (time_df
        .withColumn("hour_24", (col("minute") / 60).cast("int"))
        .withColumn("minute_val", (col("minute") % 60).cast("int"))
        .withColumn("time_key", col("hour_24") * 100 + col("minute_val"))
        .withColumn("hour_12", 
            when(col("hour_24") == 0, 12)
            .when(col("hour_24") > 12, col("hour_24") - 12)
            .otherwise(col("hour_24")))
        .withColumn("am_pm", when(col("hour_24") < 12, "AM").otherwise("PM"))
        .withColumn("time_value", 
            concat(
                lpad(col("hour_24").cast("string"), 2, "0"),
                lit(":"),
                lpad(col("minute_val").cast("string"), 2, "0")
            ))
        .withColumn("time_period", 
            when(col("hour_24").between(0, 5), "Night")
            .when(col("hour_24").between(6, 11), "Morning")
            .when(col("hour_24").between(12, 17), "Afternoon")
            .when(col("hour_24").between(18, 21), "Evening")
            .otherwise("Late Night"))
        .withColumn("is_peak_hour", 
            col("hour_24").between(11, 14) | col("hour_24").between(18, 21))
        .select("time_key", "time_value", "hour_24", "hour_12", "am_pm", 
                col("minute_val").alias("minute"), "time_period", "is_peak_hour")
    )
    
    return dim_time

dim_time_df = generate_dim_time()
dim_time_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_time")
print(f"✅ dim_time créée avec {dim_time_df.count():,} enregistrements")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table de Faits: trip_fact (avec Enrichissement Location)

# COMMAND ----------

# Lire trip_events silver
trip_silver_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.trip_events_silver")

# Lire dim_location pour mapper les adresses
dim_location_df = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_location")
eater_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.eater_silver")
merchant_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.merchant_silver")

# Join pour obtenir pickup_location_id (merchant address)
trip_with_pickup = trip_silver_df.join(
    merchant_df.select("merchant_id", "address_line_1", "city", "postal_code"),
    "merchant_id",
    "left"
).join(
    dim_location_df.filter(col("location_type") == "pickup")
        .select(col("location_id").alias("pickup_location_id"), 
                col("address_line_1").alias("pickup_addr"),
                col("city").alias("pickup_city")),
    (col("address_line_1") == col("pickup_addr")) & (col("city") == col("pickup_city")),
    "left"
).drop("address_line_1", "city", "postal_code", "pickup_addr", "pickup_city")

# Join pour obtenir dropoff_location_id (eater address)
trip_with_locations = trip_with_pickup.join(
    eater_df.select("eater_id", "address_line_1", "city", "postal_code"),
    "eater_id",
    "left"
).join(
    dim_location_df.filter(col("location_type") == "dropoff")
        .select(col("location_id").alias("dropoff_location_id"),
                col("address_line_1").alias("dropoff_addr"),
                col("city").alias("dropoff_city"),
                col("region_zone")),
    (col("address_line_1") == col("dropoff_addr")) & (col("city") == col("dropoff_city")),
    "left"
).drop("address_line_1", "city", "postal_code", "dropoff_addr", "dropoff_city")

# Agrégation par trip pour obtenir les timestamps de chaque événement
trip_aggregated = (trip_with_locations
    .groupBy("trip_id", "order_id", "eater_id", "merchant_id", "courier_id",
             "pickup_location_id", "dropoff_location_id", "region_zone")
    .agg(
        spark_max(when(col("event_type") == "order_placed", col("event_time"))).alias("order_placed_at"),
        spark_max(when(col("event_type") == "order_accepted", col("event_time"))).alias("order_accepted_at"),
        spark_max(when(col("event_type") == "courier_dispatched", col("event_time"))).alias("courier_dispatched_at"),
        spark_max(when(col("event_type") == "pickup_arrived", col("event_time"))).alias("pickup_arrived_at"),
        spark_max(when(col("event_type") == "pickup_completed", col("event_time"))).alias("pickup_completed_at"),
        spark_max(when(col("event_type") == "dropoff_arrived", col("event_time"))).alias("dropoff_arrived_at"),
        spark_max(when(col("event_type") == "delivered", col("event_time"))).alias("delivered_at"),
        spark_max(when(col("event_type") == "cancelled", col("event_time"))).alias("cancelled_at"),
        # Prendre les valeurs du dernier événement (les colonnes sont dans Silver maintenant)
        spark_max(col("subtotal_amount")).alias("subtotal_amount"),
        spark_max(col("delivery_fee")).alias("delivery_fee"),
        spark_max(col("service_fee")).alias("service_fee"),
        spark_max(col("tax_amount")).alias("tax_amount"),
        spark_max(col("tip_amount")).alias("tip_amount"),
        spark_max(col("total_amount")).alias("total_amount"),
        spark_max(col("distance_miles")).alias("distance_miles"),
        spark_max(col("preparation_time_minutes")).alias("preparation_time_minutes"),
        spark_max(col("delivery_time_minutes")).alias("delivery_time_minutes"),
        spark_max(col("is_group_order")).alias("is_group_order"),
        spark_max(col("promo_code_used")).alias("promo_code_used"),
        spark_max(col("discount_amount")).alias("discount_amount"),
        spark_max(col("eater_rating")).alias("eater_rating"),
        spark_max(col("courier_rating")).alias("courier_rating"),
        spark_max(col("merchant_rating")).alias("merchant_rating"),
        spark_max(col("weather_condition")).alias("weather_condition"),
        spark_max(col("event_time")).alias("updated_at")
    )
).withColumn(
    # Calculer date_partition depuis order_placed_at
    "date_partition",
    to_date(col("order_placed_at"))
).withColumn(
    # Calculer trip_status depuis les timestamps d'événements
    "trip_status",
    when(col("cancelled_at").isNotNull(), "cancelled")
    .when(col("delivered_at").isNotNull(), "completed")
    .when(col("dropoff_arrived_at").isNotNull(), "in_delivery")
    .when(col("pickup_completed_at").isNotNull(), "picked_up")
    .when(col("courier_dispatched_at").isNotNull(), "dispatched")
    .when(col("order_accepted_at").isNotNull(), "accepted")
    .otherwise("pending")
).withColumn(
    # Calculer total_time_minutes (order_placed → delivered)
    "total_time_minutes",
    when(col("delivered_at").isNotNull(), 
         (col("delivered_at").cast("long") - col("order_placed_at").cast("long")) / 60)
    .otherwise(None)
).withColumn(
    # Calculer courier_payout (15% de total_amount pour simplifier)
    "courier_payout",
    when(col("total_amount").isNotNull(), col("total_amount") * 0.15)
    .otherwise(None)
)

# Préparer trip_fact avec valeurs par défaut
trip_fact_source = trip_aggregated.select(
    col("trip_id"),
    col("order_id"),
    col("eater_id"),
    col("merchant_id"),
    col("courier_id"),
    coalesce(col("pickup_location_id"), lit(0)).alias("pickup_location_id"),
    coalesce(col("dropoff_location_id"), lit(0)).alias("dropoff_location_id"),
    col("order_placed_at"),
    col("order_accepted_at"),
    col("courier_dispatched_at"),
    col("pickup_arrived_at"),
    col("pickup_completed_at"),
    col("dropoff_arrived_at"),
    col("delivered_at"),
    coalesce(col("subtotal_amount"), lit(0)).alias("subtotal_amount"),
    coalesce(col("delivery_fee"), lit(0)).alias("delivery_fee"),
    coalesce(col("service_fee"), lit(0)).alias("service_fee"),
    coalesce(col("tax_amount"), lit(0)).alias("tax_amount"),
    coalesce(col("tip_amount"), lit(0)).alias("tip_amount"),
    coalesce(col("total_amount"), lit(0)).alias("total_amount"),
    coalesce(col("courier_payout"), lit(0)).alias("courier_payout"),
    coalesce(col("distance_miles"), lit(0)).alias("distance_miles"),
    coalesce(col("preparation_time_minutes"), lit(0)).alias("preparation_time_minutes"),
    coalesce(col("delivery_time_minutes"), lit(0)).alias("delivery_time_minutes"),
    coalesce(col("total_time_minutes"), lit(0)).alias("total_time_minutes"),
    col("trip_status"),
    lit(1).alias("version_number"),
    coalesce(col("is_group_order"), lit(False)).alias("is_group_order"),
    col("promo_code_used"),
    coalesce(col("discount_amount"), lit(0)).alias("discount_amount"),
    col("eater_rating"),
    col("courier_rating"),
    col("merchant_rating"),
    col("date_partition"),
    coalesce(col("region_zone"), lit("other")).alias("region_partition"),
    col("weather_condition"),
    col("updated_at")
)

# Vérifier si trip_fact existe
try:
    trip_fact_delta = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA_GOLD}.trip_fact")
    table_exists = True
except:
    table_exists = False

if not table_exists:
    # Première charge
    trip_fact_source.write.format("delta") \
        .partitionBy("date_partition", "region_partition") \
        .mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.trip_fact")
    print(f"✅ trip_fact créée avec {trip_fact_source.count():,} enregistrements")
else:
    # MERGE pour upserts
    trip_fact_delta.alias("target").merge(
        trip_fact_source.alias("source"),
        "target.trip_id = source.trip_id"
    ).whenMatchedUpdate(
        condition="source.updated_at > target.updated_at",
        set={col: f"source.{col}" for col in trip_fact_source.columns if col not in ["trip_id"]}
    ).whenNotMatchedInsertAll().execute()
    
    print(f"✅ trip_fact mis à jour via MERGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimisation des Tables Gold

# COMMAND ----------

# OPTIMIZE et Z-ORDER pour performance
tables_to_optimize = [
    f"{CATALOG}.{SCHEMA_GOLD}.dim_eater",
    f"{CATALOG}.{SCHEMA_GOLD}.dim_merchant",
    f"{CATALOG}.{SCHEMA_GOLD}.dim_courier",
    f"{CATALOG}.{SCHEMA_GOLD}.dim_location",
    f"{CATALOG}.{SCHEMA_GOLD}.trip_fact"
]

for table in tables_to_optimize:
    print(f"🔧 Optimizing {table}...")
    spark.sql(f"OPTIMIZE {table}")

# Z-Order sur trip_fact pour queries fréquentes
spark.sql(f"""
    OPTIMIZE {CATALOG}.{SCHEMA_GOLD}.trip_fact
    ZORDER BY (eater_id, merchant_id, courier_id, order_placed_at)
""")

print("✅ Optimisation terminée")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistiques Finales et Validation

# COMMAND ----------

tables = [
    "dim_eater", "dim_merchant", "dim_courier", 
    "dim_location", "dim_date", "dim_time", "trip_fact"
]

print("\n" + "="*60)
print("📊 STATISTIQUES DATA WAREHOUSE GOLD LAYER")
print("="*60)

for table in tables:
    count = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.{table}").count()
    print(f"  {table:20} : {count:>10,} enregistrements")

print("="*60)
print("✅ Pipeline Gold exécuté avec succès!")
print("="*60)
