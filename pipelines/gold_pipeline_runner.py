# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Pipeline Runner - Wrapper pour Databricks Job
# MAGIC Ce notebook exécute le gold_pipeline.py avec les paramètres fournis par le Job

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Récupérer les paramètres du Job

# COMMAND ----------

# Récupérer les paramètres avec gestion d'erreur
try:
    catalog = dbutils.widgets.get("catalog")
except:
    catalog = "ubear_catalog"

try:
    bronze_schema = dbutils.widgets.get("bronze_schema")
except:
    bronze_schema = "ubear_bronze"

try:
    silver_schema = dbutils.widgets.get("silver_schema")
except:
    silver_schema = "ubear_silver"

try:
    gold_schema = dbutils.widgets.get("gold_schema")
except:
    gold_schema = "ubear_gold"

print(f"🔧 Configuration chargée:")
print(f"   - Catalog: {catalog}")
print(f"   - Bronze Schema: {bronze_schema}")
print(f"   - Silver Schema: {silver_schema}")
print(f"   - Gold Schema: {gold_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Définir la configuration Spark

# COMMAND ----------

spark.conf.set("catalog", catalog)
spark.conf.set("schema.bronze", bronze_schema)
spark.conf.set("schema.silver", silver_schema)
spark.conf.set("schema.gold", gold_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Exécuter le Gold Pipeline

# COMMAND ----------

# Importer le gold_pipeline
%run ./gold_pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Confirmation d'exécution

# COMMAND ----------

print("✅ Gold Pipeline exécuté avec succès!")
print(f"   Timestamp: {spark.sql('SELECT current_timestamp() as ts').collect()[0][0]}")
