# Exemple Spark SCD2 pour la dimension eater
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.appName("UpdateDimEaterSCD2").getOrCreate()

# Charger la table silver (source) et gold (cible)
src = spark.read.format("delta").load("/opt/spark-apps/delta/silver/dim_eater")
tgt = DeltaTable.forPath(spark, "/opt/spark-apps/delta/gold/dim_eater")

# Exemple de logique SCD2 (simplifiée)
updates = src.alias("src").join(
    tgt.toDF().alias("tgt"),
    col("src.eater_uuid") == col("tgt.eater_uuid")
).where(
    (col("src.hash") != col("tgt.hash")) & (col("tgt.is_current") == True)
)

# Fermer les anciennes versions
if updates.count() > 0:
    tgt.alias("tgt").merge(
        updates.alias("src"),
        "tgt.eater_uuid = src.eater_uuid AND tgt.is_current = true"
    ).whenMatchedUpdate(set={
        "effective_end_date": current_timestamp(),
        "is_current": "false"
    }).execute()

    # Insérer la nouvelle version
    src.withColumn("effective_start_date", current_timestamp()) \
       .withColumn("effective_end_date", None) \
       .withColumn("is_current", True) \
       .withColumn("version_number", col("tgt.version_number") + 1) \
       .write.format("delta").mode("append").save("/opt/spark-apps/delta/gold/dim_eater")
