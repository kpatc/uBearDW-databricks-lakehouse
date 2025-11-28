# Databricks PySpark Notebook: SCD2 sur dim_merchant (MERGE Delta)
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws
from pyspark.sql.window import Window
import pyspark.sql.functions as F

silver_table = "workspace.default.merchant_silver"
gold_table = "workspace.default.dim_merchant"

silver_df = spark.read.format("delta").table(silver_table)
try:
    gold_df = spark.read.format("delta").table(gold_table)
except Exception:
    gold_df = None

business_keys = ["merchant_id"]
attribute_cols = [c for c in silver_df.columns if c not in business_keys]

silver_df = silver_df.withColumn(
    "row_hash",
    sha2(concat_ws("||", *[col(c).cast("string") for c in attribute_cols]), 256)
)

if gold_df is not None:
    gold_df = gold_df.withColumn(
        "row_hash",
        sha2(concat_ws("||", *[col(c).cast("string") for c in attribute_cols]), 256)
    )
    join_cond = [silver_df[k] == gold_df[k] for k in business_keys]
    changes = silver_df.join(gold_df, join_cond, "left_outer") \
        .where((gold_df["row_hash"].isNull()) | (silver_df["row_hash"] != gold_df["row_hash"]))
else:
    changes = silver_df

def scd2_columns(df):
    return df \
        .withColumn("is_current", lit(True)) \
        .withColumn("valid_from", current_timestamp()) \
        .withColumn("valid_to", lit(None).cast("timestamp"))

new_records = scd2_columns(changes)

if gold_df is not None:
    window = Window.partitionBy(*business_keys).orderBy(F.desc("valid_from"))
    expired = gold_df.join(new_records, business_keys, "inner") \
        .where(gold_df["is_current"] == True) \
        .withColumn("is_current", lit(False)) \
        .withColumn("valid_to", current_timestamp())
    final_df = new_records.unionByName(expired.select(new_records.columns))
    unchanged = gold_df.join(new_records, business_keys, "left_anti")
    final_df = final_df.unionByName(unchanged.select(new_records.columns))
else:
    final_df = new_records

final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table)
