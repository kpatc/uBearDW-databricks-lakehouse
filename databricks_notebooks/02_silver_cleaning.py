# Databricks PySpark Notebook: Bronze → Silver (nettoyage, parsing, déduplication)
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# Schéma du payload (adapter selon la fact table)
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

bronze_table = "workspace.default.trip_events_bronze"
bronze_df = spark.read.format("delta").table(bronze_table)

silver_df = bronze_df.withColumn("payload_json", from_json(col("payload"), payload_schema))
for field in payload_schema.fieldNames():
    silver_df = silver_df.withColumn(field, col(f"payload_json.{field}"))
silver_df = silver_df.drop("payload_json")

silver_df = silver_df.dropDuplicates(["order_id", "event_time"])

silver_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.trip_events_silver")
