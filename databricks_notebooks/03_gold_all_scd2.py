# Databricks PySpark Notebook: SCD2 for dimensions (merchant, eater, courier) + MERGE for trip_fact
from pyspark.sql.functions import col, sha2, concat_ws, current_timestamp, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Define dimensions and their silver/gold table names
dimensions = {
    'merchant': {
        'silver_table': 'workspace.default.merchant_silver',
        'gold_table': 'workspace.default.dim_merchant',
        'business_keys': ['merchant_id']
    },
    'eater': {
        'silver_table': 'workspace.default.eater_silver',
        'gold_table': 'workspace.default.dim_eater',
        'business_keys': ['eater_id']
    },
    'courier': {
        'silver_table': 'workspace.default.courier_silver',
        'gold_table': 'workspace.default.dim_courier',
        'business_keys': ['courier_id']
    }
}

for dim_name, meta in dimensions.items():
    silver_table = meta['silver_table']
    gold_table = meta['gold_table']

    silver_df = spark.read.format('delta').table(silver_table)
    try:
        gold_df = spark.read.format('delta').table(gold_table)
    except Exception:
        gold_df = None

    business_keys = meta['business_keys']
    scd_cols = ['effective_start_date', 'effective_end_date', 'is_current', 'version_number', 'row_hash']
    attribute_cols = [c for c in silver_df.columns if c not in business_keys + scd_cols]

    # Add hash & join with current gold to calculate version increment
    silver_df = silver_df.withColumn('row_hash', sha2(concat_ws('||', *[col(c).cast('string') for c in attribute_cols]), 256))

    if gold_df is not None:
      current_gold_df = gold_df.filter(col('is_current') == True)
    else:
      current_gold_df = None

    # Compute previous version from current gold (if exists)
    if current_gold_df is not None:
      prev_versions = current_gold_df.select(*business_keys, col('version_number').alias('prev_version'))
      source_with_prev = silver_df.join(prev_versions, on=business_keys, how='left')
    else:
      source_with_prev = silver_df.withColumn('prev_version', lit(0))

    # New records to insert
    new_records = source_with_prev.withColumn('is_current', lit(True)) \
                    .withColumn('effective_start_date', current_timestamp()) \
                    .withColumn('effective_end_date', lit(None).cast('timestamp')) \
                    .withColumn('version_number', (col('prev_version').cast('int') + 1))

    if gold_df is not None:
      # Expire old records where row_hash differs
      expired = gold_df.join(new_records, business_keys, 'inner') \
        .where((gold_df['is_current'] == True) & (gold_df['row_hash'] != new_records['row_hash'])) \
        .withColumn('is_current', lit(False)) \
        .withColumn('effective_end_date', current_timestamp())

      # Unchanged records - keep those current rows without changes
      unchanged = gold_df.join(new_records, business_keys, 'left_anti')

      # Final set: existing unchanged + expired + new records
      final_df = unchanged.unionByName(expired.select(unchanged.columns)).unionByName(new_records.select(unchanged.columns))
    else:
      final_df = new_records

    # Write final SCD2 dataset back (overwrite mode ensures current snapshot is replaced)
    final_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(gold_table)

# Now perform fact MERGE for trip_fact: merge silver_trip_fact into gold trip_fact (SCD2-like but single row per key with latest event_time)
# For demo, assume trip_fact_silver exists; ensure fields exist
try:
    trip_silver_df = spark.read.format('delta').table('workspace.default.trip_events_silver')
    trip_silver_df.createOrReplaceTempView('trip_silver')
    fact_table = 'workspace.default.trip_fact'

    # Merge based on order_id
    fact_merge_sql = f"""
    MERGE INTO {fact_table} AS target
    USING (
      SELECT * FROM trip_silver
    ) AS source
    ON target.order_id = source.order_id
    WHEN MATCHED AND (target.updated_at IS NULL OR target.updated_at < source.event_time) THEN
      UPDATE SET target.total_amount = source.total_amount, target.updated_at = source.event_time
    WHEN NOT MATCHED THEN
      INSERT (order_id, eater_id, merchant_id, courier_id, total_amount, updated_at)
      VALUES (source.order_id, source.eater_id, source.merchant_id, source.courier_id, source.total_amount, source.event_time)
    """

    print(f"Running fact MERGE: \n{fact_merge_sql}")
    spark.sql(fact_merge_sql)
except Exception as e:
  print('Trip fact MERGE skipped or failed:', e)
