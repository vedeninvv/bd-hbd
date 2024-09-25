import time

import psycopg2
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

conn_params = {
    'host': "172.18.208.1",
    'dbname': "postgres",
    'user': "postgres",
    'password': "postgres",
    'port': "5432",
}


def load_postgres_data_to_warehouse(source_table, **kwargs):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE staging.pg_{source_table}")
    conn.commit()
    cursor.close()

    cursor = conn.cursor()
    insert_query = f"""
        INSERT INTO staging.pg_{source_table} 
        SELECT *, now() as inserted_at, 'False' as flag FROM source.{source_table};
        """

    cursor.execute(insert_query)
    conn.commit()
    cursor.close()
    conn.close()


def get_max_timestamp():
    """ Обновление таблицы `staging.settings` """

    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    cursor.execute(f"SELECT max(time) FROM source.logs")
    results = cursor.fetchall()
    max_time = results[0][0]
    conn.commit()
    cursor.close()

    cursor = conn.cursor()
    cursor.execute("SELECT mongo_actual_time FROM staging.settings LIMIT 1")
    try:
        mongo_value = cursor.fetchone()[0]
        if mongo_value is None:
            mongo_value = 'NULL'
    except TypeError:  # Если объект None 
        mongo_value = 'NULL'
    finally:
        print('mongo_value: ', mongo_value)
        cursor.execute("TRUNCATE TABLE staging.settings")
        if mongo_value != 'NULL':
            cursor.execute(f"INSERT INTO staging.settings VALUES ('{max_time}', '{mongo_value}')")
        else:
            cursor.execute(f"INSERT INTO staging.settings VALUES ('{max_time}', {mongo_value})")
        conn.commit()
        cursor.close()
        conn.close()


def delay_execution():
    time.sleep(
        10)


default_args = {
    'owner': 'vv',
    'start_date': days_ago(1)
}

with DAG(
        'postgres_to_stg_init',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    delay_operator = PythonOperator(
        task_id='delay_task',
        python_callable=delay_execution,
        dag=dag
    )

    load_category = PythonOperator(
        task_id='load_category',
        python_callable=load_postgres_data_to_warehouse,
        dag=dag,
        op_kwargs={'source_table': 'category'},
        trigger_rule='all_success'
    )

    load_dish = PythonOperator(
        task_id='load_dish',
        python_callable=load_postgres_data_to_warehouse,
        dag=dag,
        op_kwargs={'source_table': 'dish'},
        trigger_rule='all_success'
    )

    load_payment = PythonOperator(
        task_id='load_payment',
        python_callable=load_postgres_data_to_warehouse,
        dag=dag,
        op_kwargs={'source_table': 'payment'},
        trigger_rule='all_success'
    )

    load_client = PythonOperator(
        task_id='load_client',
        python_callable=load_postgres_data_to_warehouse,
        dag=dag,
        op_kwargs={'source_table': 'client'},
        trigger_rule='all_success'
    )

    update_settings = PythonOperator(
        task_id='update_settings',
        python_callable=get_max_timestamp,
        dag=dag,
        trigger_rule='all_success'
    )

    end_load = DummyOperator(
        task_id='end_load',
        dag=dag
    )

    delay_operator >> [load_client, load_payment, load_dish, load_category] >> update_settings >> end_load