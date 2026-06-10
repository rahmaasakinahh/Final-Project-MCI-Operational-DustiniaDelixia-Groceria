from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/dags/scripts')

from extract import extract
from transform import transform
from validate import validate
from load import load
from ge_validate import ge_validate

default_args = {
    'owner'       : 'dustinia_mci',
    'start_date'  : datetime(2024, 1, 1),
    'retries'     : 1,
    'retry_delay' : timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='dustinia_operational_pipeline',
    default_args=default_args,
    description='CSV → Extract → Transform → Validate → Load ClickHouse',
    schedule_interval='@weekly',
    catchup=False,
    max_active_runs=1,
    tags=['dustinia', 'operational', 'mci2026']
) as dag:

    start = EmptyOperator(task_id='start')

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    task_validate = PythonOperator(
        task_id='validate',
        python_callable=validate,
    )

    task_load = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load,
    )

    task_ge_validate = PythonOperator(
        task_id='ge_validate',
        python_callable=ge_validate,
    )

    end = EmptyOperator(task_id='end')

    # urutan eksekusi
    start >> task_extract >> task_transform >> task_validate >> task_load >> task_ge_validate >> end
