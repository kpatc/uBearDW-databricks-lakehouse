# PySpark notebook cell pour Databricks : Silver → Gold (SCD2, MERGE, agrégats)
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

# Charger la table silver (source) et gold (cible)
src = spark.read.table("workspace.default.trip_events_silver")
tgt = DeltaTable.forName(spark, "workspace.default.trip_fact")

# Exemple de logique SCD2 (simplifiée, à adapter)
updates = src.alias("src").join(
    tgt.toDF().alias("tgt"),
    col("src.order_id") == col("tgt.order_id")
).where(
    (col("src.hash") != col("tgt.hash")) & (col("tgt.is_current") == True)
)

if updates.count() > 0:
    tgt.alias("tgt").merge(
        updates.alias("src"),
        "tgt.order_id = src.order_id AND tgt.is_current = true"
    ).whenMatchedUpdate(set={
        "effective_end_date": current_timestamp(),
        "is_current": "false"
    }).execute()

    # Insérer la nouvelle version
    src.withColumn("effective_start_date", current_timestamp()) \
       .withColumn("effective_end_date", None) \
       .withColumn("is_current", True) \
       .withColumn("version_number", col("tgt.version_number") + 1) \
       .write.format("delta").mode("append").saveAsTable("workspace.default.trip_fact")
