# Databricks PySpark Notebook: Bronze → Silver (nettoyage, parsing, déduplication)
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# Schéma du payload (adapter selon la fact table)
# Payload schema for trip_events 'after' map (we keep string-cast and parse JSON)
payload_schema = StructType() \
    .add("order_id", StringType()) \
    .add("eater_id", StringType()) \
    .add("merchant_id", StringType()) \
    .add("courier_id", StringType()) \
    .add("pickup_location_id", StringType()) \
    .add("dropoff_location_id", StringType()) \
    .add("order_placed_at", TimestampType()) \
    .add("order_accepted_at", TimestampType()) \
    .add("courier_dispatched_at", TimestampType()) \
    .add("pickup_arrived_at", TimestampType()) \
    .add("pickup_completed_at", TimestampType()) \
    .add("dropoff_arrived_at", TimestampType()) \
    .add("delivered_at", TimestampType()) \
    .add("subtotal_amount", DecimalType(12,2)) \
    .add("delivery_fee", DecimalType(12,2)) \
    .add("service_fee", DecimalType(12,2)) \
    .add("tax_amount", DecimalType(12,2)) \
    .add("tip_amount", DecimalType(12,2)) \
    .add("total_amount", DecimalType(12,2)) \
    .add("courier_payout", DecimalType(12,2)) \
    .add("distance_miles", DoubleType()) \
    .add("preparation_time_minutes", IntegerType()) \
    .add("delivery_time_minutes", IntegerType()) \
    .add("total_time_minutes", IntegerType()) \
    .add("trip_status", StringType()) \
    .add("cancellation_reason", StringType()) \
    .add("is_scheduled_order", BooleanType()) \
    .add("is_group_order", BooleanType()) \
    .add("promo_code_used", StringType()) \
    .add("discount_amount", DecimalType(12,2)) \
    .add("eater_rating", IntegerType()) \
    .add("courier_rating", IntegerType()) \
    .add("merchant_rating", IntegerType()) \
    .add("date_partition", StringType()) \
    .add("region_partition", StringType()) \
    .add("weather_condition", StringType())

bronze_trip_table = "workspace.default.dbserver1_public_trip_events_bronze"
bronze_merchant_table = "workspace.default.dbserver1_public_merchant_bronze"
bronze_eater_table = "workspace.default.dbserver1_public_eater_bronze"

bronze_trip_df = spark.read.format("delta").table(bronze_trip_table)

silver_df = bronze_trip_df.withColumn("payload_json", from_json(col("payload"), payload_schema))
for field in payload_schema.fieldNames():
    silver_df = silver_df.withColumn(field, col(f"payload_json.{field}"))
silver_df = silver_df.drop("payload_json")

silver_df = silver_df.dropDuplicates(["order_id", "event_time"])

silver_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.trip_events_silver")

# Merchant bronze -> silver
merchant_schema = StructType() \
    .add("merchant_id", StringType()) \
    .add("merchant_uuid", StringType()) \
    .add("merchant_name", StringType()) \
    .add("city", StringType()) \
    .add("cuisine_type", StringType())

merchant_bronze_df = spark.read.format("delta").table(bronze_merchant_table)
merchant_silver_df = merchant_bronze_df.withColumn("payload_json", from_json(col("payload"), merchant_schema))
for field in merchant_schema.fieldNames():
    merchant_silver_df = merchant_silver_df.withColumn(field, col(f"payload_json.{field}"))
merchant_silver_df = merchant_silver_df.drop("payload_json").dropDuplicates(["merchant_id"])
merchant_silver_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.merchant_silver")

# Eater bronze -> silver
eater_schema = StructType() \
    .add("eater_id", StringType()) \
    .add("eater_uuid", StringType()) \
    .add("first_name", StringType()) \
    .add("last_name", StringType()) \
    .add("email", StringType())

eater_bronze_df = spark.read.format("delta").table(bronze_eater_table)
eater_silver_df = eater_bronze_df.withColumn("payload_json", from_json(col("payload"), eater_schema))
for field in eater_schema.fieldNames():
    eater_silver_df = eater_silver_df.withColumn(field, col(f"payload_json.{field}"))
eater_silver_df = eater_silver_df.drop("payload_json").dropDuplicates(["eater_id"])
eater_silver_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.eater_silver")
