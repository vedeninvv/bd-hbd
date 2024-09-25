from airflow import DAG
from pymongo import MongoClient
import psycopg2
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
import datetime
from dbuilder import MongoBuilder


mongo_url = 'mongodb://172.18.208.1:27017/'
mongo_db = 'mongo'
conn_params = {
    'host': "172.18.208.1",
    'dbname': "postgres",
    'user': "postgres",
    'password': "postgres",
    'port': "5432",
}


def cleanup_mongo_tables():
    """ Удаляем вспомогательные таблицы монги, возникшие в результате вложенности документа. Например `mongo_menu` """
    
    conn = psycopg2.connect(**conn_params)
    based_collections = ['orders', 'restaurants', 'clients'] # Изначальные коллекции
    query = "select table_name from information_schema.tables where table_schema = 'staging' and table_name LIKE '%mongo%';"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    mongo_tables = [results[i][0] for i in range(len(results))]
    print(mongo_tables)
    for tb in mongo_tables:
        if tb[6:] != 'orders' and tb[6:] != 'restaurants' and tb[6:] != 'clients': # Можно получше переписать на питоне с помощью map, но забил
            cursor = conn.cursor()
            cursor.execute(f"drop table staging.{tb}")
            conn.commit()
            cursor.close()
    conn.close()


def load_data_from_mongo_to_postgres(collection_name, **kwargs):
    
    client = MongoClient(mongo_url)
    db = client[mongo_db]
    
    conn = psycopg2.connect(**conn_params)
    collection = db[collection_name]

    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE staging.mongo_{collection_name}")
    conn.commit()
    cursor.close()

    mongo_to_postgre = MongoBuilder(collection, conn, collection_name, is_delta= False)
    mongo_to_postgre.build_mongo_staging()
        
    conn.commit()
    conn.close()


def get_max_timestamp(collection_list):
    """ Обновление таблицы `staging.settings` """

    client = MongoClient(mongo_url)
    db = client[mongo_db]

    # Ищем максимальный `update_time` во всех коллекциях, чтобы вставить его в `staging.settings.mongo_actual_time`
    max_timestamp = datetime.datetime.strptime('2010-04-22 10:07:02', '%Y-%m-%d %H:%M:%S')
    for collection_name in collection_list:
        collection = db[collection_name]
        documents = collection.find()
        for doc in documents:
            if '.' in str(doc['update_time']):
                doc['update_time'] = str(doc['update_time'])[:-7] # Есть timestamp вида 2010-04-22 10:07:02.100000 Убираем лишнее
            print(str(doc['update_time']))
            if datetime.datetime.strptime(str(doc['update_time']), '%Y-%m-%d %H:%M:%S') > max_timestamp:
                max_timestamp = datetime.datetime.strptime(str(doc['update_time']), '%Y-%m-%d %H:%M:%S')

    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    # Достаём `postgres_actual_time`, после транкейта кладём обратно в `staging.settings`
    cursor.execute("SELECT postgres_actual_time FROM staging.settings LIMIT 1")
    try:
        first_value = cursor.fetchone()[0]
        if first_value is None:
            first_value = 'NULL'
    except TypeError:  # Если объект None 
        first_value = 'NULL'
    finally:
        print('postgre_value: ', first_value)
        cursor.execute("TRUNCATE TABLE staging.settings")
        if first_value != 'NULL':
            cursor.execute(f"INSERT INTO staging.settings VALUES ('{first_value}', '{max_timestamp}')")
        else:
            cursor.execute(f"INSERT INTO staging.settings VALUES ({first_value}, '{max_timestamp}')")
        conn.commit()
        cursor.close()
        conn.close()

default_args = {
    'owner': 'vv',
    'start_date': days_ago(1)
}

with DAG(
    'mongo_to_stg_init',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    load_clients = PythonOperator(
        task_id='load_clients',
        python_callable=load_data_from_mongo_to_postgres,
        dag=dag,
        op_kwargs={'collection_name': 'clients'},
        trigger_rule='all_success'
    )

    load_orders = PythonOperator(
        task_id='load_orders',
        python_callable=load_data_from_mongo_to_postgres,
        dag=dag,
        op_kwargs={'collection_name': 'orders'},
        trigger_rule='all_success'
    )

    load_restaurants = PythonOperator(
        task_id='load_restaurants',
        python_callable=load_data_from_mongo_to_postgres,
        dag=dag,
        op_kwargs={'collection_name': 'restaurants'},
        trigger_rule='all_success'
    )

    update_settings = PythonOperator(
        task_id='update_settings',
        python_callable=get_max_timestamp,
        dag=dag,
        op_kwargs={'collection_list': ['restaurants', 'orders', 'clients']},
        trigger_rule='all_success'
    )

    drop_tables = PythonOperator(
        task_id='drop_support_tables',
        python_callable=cleanup_mongo_tables,
        dag=dag
    )

    end_load = DummyOperator(
        task_id='end_load',
        dag=dag
    )

    drop_tables >> [load_clients, load_orders, load_restaurants] >> update_settings >> end_load