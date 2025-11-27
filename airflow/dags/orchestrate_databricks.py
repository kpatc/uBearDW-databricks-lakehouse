from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'orchestrate_databricks',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # 2 AM UTC
    catchup=False
)

run_bronze = DatabricksRunNowOperator(
    task_id='run_bronze_stream',
    databricks_conn_id='databricks_default',
    job_id=123,  # Remplace par l'ID du job Databricks pour bronze
    dag=dag
)

run_silver = DatabricksRunNowOperator(
    task_id='run_silver_batch',
    databricks_conn_id='databricks_default',
    job_id=124,  # Remplace par l'ID du job Databricks pour silver
    dag=dag
)

run_gold = DatabricksRunNowOperator(
    task_id='run_gold_batch',
    databricks_conn_id='databricks_default',
    job_id=125,  # Remplace par l'ID du job Databricks pour gold
    dag=dag
)

run_data_quality = DatabricksRunNowOperator(
    task_id='run_data_quality',
    databricks_conn_id='databricks_default',
    job_id=126,  # Remplace par l'ID du job Databricks pour data quality
    dag=dag
)

run_bronze >> run_silver >> run_gold >> run_data_quality
