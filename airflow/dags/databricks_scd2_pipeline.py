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

dag = DAG(
    'databricks_scd2_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # 2 AM UTC
    catchup=False
)

# SubmitRun operator calling notebooks in Databricks. Replace cluster_id and notebook_paths with your values
cluster_id = 'YOUR_CLUSTER_ID'

bronze = DatabricksSubmitRunOperator(
    task_id='bronze_ingestion',
    databricks_conn_id='databricks_default',
    json={
        'existing_cluster_id': cluster_id,
        'notebook_task': {
            'notebook_path': '/Repos/your-org/databricks_notebooks/01_stream_trip_events_bronze'
        }
    },
    dag=dag
)
silver = DatabricksSubmitRunOperator(
    task_id='silver_cleaning',
    databricks_conn_id='databricks_default',
    json={
        'existing_cluster_id': cluster_id,
        'notebook_task': {
            'notebook_path': '/Repos/your-org/databricks_notebooks/02_silver_cleaning'
        }
    },
    dag=dag
)
gold = DatabricksSubmitRunOperator(
    task_id='gold_dim_all_scd2',
    databricks_conn_id='databricks_default',
    json={
        'existing_cluster_id': cluster_id,
        'notebook_task': {
            'notebook_path': '/Repos/your-org/databricks_notebooks/03_gold_all_scd2'
        }
    },
    dag=dag
)
data_quality = DatabricksSubmitRunOperator(
    task_id='data_quality_trip_fact',
    databricks_conn_id='databricks_default',
    json={
        'existing_cluster_id': cluster_id,
        'notebook_task': {
            'notebook_path': '/Repos/your-org/databricks_notebooks/04_data_quality_trip_fact'
        }
    },
    dag=dag
)

bronze >> silver >> gold >> data_quality
