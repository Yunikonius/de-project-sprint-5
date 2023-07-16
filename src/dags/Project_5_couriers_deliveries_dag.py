from logging import Logger
import psycopg2
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from Project_5_all_tables_creation import non_existing_tables_creation
from Project_5_restaraunts_api_loader import restaurants_loader_from_src

conn = psycopg2.connect(
                host="localhost",
                port="15432",
                database="de",
                user="jovyan",
                password="jovyan"
        )
url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
nickname = "yunikonius"
cohort = 14


headers = {
        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
        "X-Nickname": nickname,
        "X-Cohort": str(cohort)
}

method_url = '/restaurants'

payload = {'sort_field': '_id', 'sort_direction': 'asc', 'limit': 50, 'offset': 0}

with DAG(
    dag_id='project5_couriers_deliveries_dag',
    schedule='0/15 * * * *',
    start_date=pendulum.datetime(2023, 7, 16, tz="UTC"),
    catchup=False,
    tags=['project5','couriers', 'deliveries'],
    is_paused_upon_creation=True
) as dag:

    schema_init = PythonOperator(
        task_id="schema_init",
        python_callable=non_existing_tables_creation()
    )

    restaurants_api = PythonOperator(
        task_id="restaurants_api_stg",
        python_callable=restaurants_loader_from_src()
    )