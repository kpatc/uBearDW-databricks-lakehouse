from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dim_scd2_update',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # 2 AM UTC
    catchup=False
)

update_dim_eater = BashOperator(
    task_id='update_dim_eater',
    bash_command='spark-submit /opt/spark-apps/update_dim_eater_scd2.py',
    dag=dag
)

update_dim_merchant = BashOperator(
    task_id='update_dim_merchant',
    bash_command='spark-submit /opt/spark-apps/update_dim_merchant_scd2.py',
    dag=dag
)

update_dim_courier = BashOperator(
    task_id='update_dim_courier',
    bash_command='spark-submit /opt/spark-apps/update_dim_courier_scd2.py',
    dag=dag
)

[update_dim_eater, update_dim_merchant, update_dim_courier]
