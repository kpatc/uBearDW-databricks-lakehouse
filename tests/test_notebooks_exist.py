import os

NOTEBOOKS = [
    'databricks_notebooks/01_stream_trip_events_bronze.py',
    'databricks_notebooks/02_silver_cleaning.py',
    'databricks_notebooks/03_gold_all_scd2.py',
    'databricks_notebooks/04_data_quality_trip_fact.py'
]


def test_notebooks_exist():
    for nb in NOTEBOOKS:
        assert os.path.exists(nb), f"Notebook {nb} not found"
