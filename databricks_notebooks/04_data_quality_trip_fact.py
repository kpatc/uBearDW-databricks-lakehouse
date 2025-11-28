# Databricks PySpark Notebook: Data Quality avec Great Expectations
import great_expectations as ge
from great_expectations.dataset import SparkDFDataset

# Charger la table fact
fact_df = spark.read.format("delta").table("workspace.default.trip_fact")
ge_df = SparkDFDataset(fact_df)

# Exemples de checks
ge_df.expect_column_values_to_not_be_null("order_id")
ge_df.expect_column_values_to_be_between("total_amount", min_value=0, max_value=1000)

results = ge_df.validate()
print(results)
