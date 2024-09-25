import psycopg2
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from dbuilder import DBuilder

conn_params = {
    'host': "172.18.208.1",
    'dbname': "postgres",
    'user': "postgres",
    'password': "postgres",
    'port': "5432",
}
base_url = "http://172.18.208.1:8001/get_data"

conn = psycopg2.connect(**conn_params)


def insert_api_data(table_name):
    """ Забираем данные с fastapi и грузим в staging """

    response = requests.get(base_url, params={"table_name": table_name})
    if response.status_code == 200:
        data = response.json()
        print(data)
    else:
        print(f"Failed to get data from {table_name} table. Status code: {response.status_code}")
        print(response.json())

    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE staging.api_{table_name}")
    conn.commit()
    cursor.close()

    # Собираем values которые необходимо вставить в staging
    values = []
    for doc in data:
        row = [str(value) for _, value in doc.items()]
        values.append(row)

    dbuilder = DBuilder(conn)
    dbuilder.insert_values('staging', 'api_' + table_name, values)

    conn.close()


default_args = {
    'owner': 'vv',
    'start_date': days_ago(1)
}

with DAG(
        'load_api_to_stg_init',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    delivery_to_stg = PythonOperator(
        task_id='delivery_to_stg',
        python_callable=insert_api_data,
        dag=dag,
        op_kwargs={'table_name': 'delivery'},
        trigger_rule='all_success'
    )

    deliveryman_to_stg = PythonOperator(
        task_id='deliveryman_to_stg',
        python_callable=insert_api_data,
        dag=dag,
        op_kwargs={'table_name': 'deliveryman'},
        trigger_rule='all_success'
    )

    start_load = DummyOperator(
        task_id='start_load',
        dag=dag
    )

    end_load = DummyOperator(
        task_id='end_load',
        dag=dag
    )

start_load >> delivery_to_stg >> deliveryman_to_stg >> end_load
