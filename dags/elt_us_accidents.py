import sys
sys.path.append("/opt/airflow/src")

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract import extract_to_bronze
from src.bronze_to_silver import bronze_to_silver
from src.silver_to_gold import silver_to_gold

default_args = {
    "owner": "helbarros",
    "start_date": datetime(2022, 1, 1),
}

with DAG(
    dag_id="elt_us_accidents",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["us_accidents", "etl"]
) as dag:

    task_extract = PythonOperator(
        task_id="extract_to_bronze",
        python_callable=extract_to_bronze,
    )

    task_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=bronze_to_silver,
    )

    task_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
    )

    task_extract >> task_silver >> task_gold
