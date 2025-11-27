# PySpark notebook cell pour Databricks : Bronze → Silver (nettoyage, parsing, déduplication)
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StringType, TimestampType

# Schéma du payload (à adapter selon le cas)
payload_schema = StructType() \
    .add("order_id", StringType()) \
    .add("eater_id", StringType()) \
    .add("merchant_id", StringType())
    # ... autres champs ...

# Lecture de la table bronze
bronze_df = spark.read.format("delta").table("workspace.default.trip_events_bronze")

# Parsing du payload JSON
silver_df = bronze_df.withColumn("payload_json", from_json(col("payload"), payload_schema))
# Exemple de déduplication
silver_df = silver_df.dropDuplicates(["event", "event_time"])  # à adapter selon la logique métier

# Ecriture dans la table silver (managed)
silver_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.trip_events_silver")
