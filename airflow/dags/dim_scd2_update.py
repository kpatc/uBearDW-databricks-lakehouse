from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# This DAG runs the SCD2 notebook (gold updates) and a data quality check daily at 2 AM UTC
with DAG(
    dag_id='dim_scd2_update',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # 2 AM UTC daily
    catchup=False,
    ) as dag:

    # Placeholder cluster id or job id; replace with your Databricks cluster/job id
    cluster_id = 'YOUR_CLUSTER_ID'

    update_gold = DatabricksSubmitRunOperator(
        task_id='update_gold_dimensions',
        databricks_conn_id='databricks_default',
        json={
            'existing_cluster_id': cluster_id,
            'notebook_task': { 'notebook_path': '/Repos/your-org/databricks_notebooks/03_gold_all_scd2' }
        }
    )

    data_quality = DatabricksSubmitRunOperator(
        task_id='data_quality_trip_fact',
        databricks_conn_id='databricks_default',
        json={
            'existing_cluster_id': cluster_id,
            'notebook_task': { 'notebook_path': '/Repos/your-org/databricks_notebooks/04_data_quality_trip_fact' }
        }
    )

    # Order: update gold dimensions, then run data quality validations
    update_gold >> data_quality
