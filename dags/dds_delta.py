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

conn = psycopg2.connect(**conn_params)

"""
В каждой таблице обновляется поле flag с `false` на `true`.
Предположим пример: Мы выгрузили дельту в dds слой. Однако, в следующей дельте мы не получили никаких новых данных.
                    Тем не менее мы всё равно выгрузим в dds данные предыдущей дельты, так как поле `inserted_at` не обновилось,
                    осталось таким же, а мы выгружаем именно по нему (`WHERE tb.inserted_at = (SELECT max(inserted_at) FROM tb)`).

Таким образом, мы вынуждены следить выгрузили ли мы уже данные по данному `inderted_at` или нет. Это достигается с помощью `flag`.
"""


def load_fact_table():
    """ Загрузка фактовой таблицы """

    insert_query = """INSERT INTO dds.Fact_Table (delivery_id, date_id, client_id, restaurant_id, deliveryman_id, order_id, valid_from, valid_to)
                        SELECT 
                            api_delivery.delivery_id,
                            Dim_Time.id,
                            mongo_orders.client,
                            mongo_orders.restaurant,
                            api_delivery.deliveryman_id,
                            api_delivery.order_id,
                            api_delivery.order_date_created,
                            NULL
                        FROM 
                            staging.api_delivery
                            JOIN staging.mongo_orders ON api_delivery.order_id = mongo_orders._id 
                            JOIN dds.Dim_Time ON api_delivery.order_date_created = Dim_Time.time_mark
                            WHERE api_delivery.inserted_at = (SELECT max(inserted_at) FROM staging.api_delivery)
                            AND mongo_orders.inserted_at = (SELECT max(inserted_at) FROM staging.mongo_orders)
                            AND api_delivery.flag = 'False' AND mongo_orders.flag = 'False';
                    """
    update_flag_query1 = """UPDATE staging.api_delivery SET flag = 'True' 
                            WHERE inserted_at = (SELECT max(inserted_at) FROM staging.api_delivery);
                        """
    update_flag_query2 = """UPDATE staging.mongo_orders SET flag = 'True' 
                            WHERE inserted_at = (SELECT max(inserted_at) FROM staging.mongo_orders);
                        """
    cursor = conn.cursor()
    cursor.execute(insert_query)
    cursor.execute(update_flag_query1)
    cursor.execute(update_flag_query2)
    conn.commit()
    cursor.close()


def load_time_dim():
    """ Загрузка таблицы времени """

    insert_query = """INSERT INTO dds.Dim_Time (time_mark, year, month, day, time)
                        SELECT 
                            order_date_created,
                            EXTRACT(YEAR FROM order_date_created)::SMALLINT,
                            EXTRACT(MONTH FROM order_date_created)::SMALLINT,
                            EXTRACT(DAY FROM order_date_created)::SMALLINT,
                            CAST(order_date_created as TIME)
                        FROM staging.api_delivery
                        WHERE inserted_at = (SELECT max(inserted_at) FROM staging.api_delivery)
                        AND flag = 'False';
                    """
    cursor = conn.cursor()
    cursor.execute(insert_query)
    conn.commit()
    cursor.close()


def load_clients_dim():
    """ Загрузка таблицы клиентов """

    insert_query = """INSERT INTO dds.Dim_Clients (client_id, name, phone, birthday, email, login, address)
                        SELECT _id, name, phone, birthday, email, login, address
                        FROM staging.mongo_clients 
                        WHERE inserted_at = (SELECT max(inserted_at) FROM staging.mongo_clients)
                        AND flag = 'False';
                    """
    update_flag_query = """UPDATE staging.mongo_clients SET flag = 'True' 
                            WHERE inserted_at = (SELECT max(inserted_at) FROM staging.mongo_clients);
                        """
    cursor = conn.cursor()
    cursor.execute(insert_query)
    cursor.execute(update_flag_query)
    conn.commit()
    cursor.close()


def load_restaurants_dim():
    """ Загрузка таблицы ресторанов """

    insert_query = """INSERT INTO dds.Dim_Restaurants (restaurant_id, name, phone, email, founding_day)
                        SELECT _id, name, phone, email, founding_day
                        FROM staging.mongo_restaurants
                        WHERE inserted_at = (SELECT max(inserted_at) FROM staging.mongo_restaurants)
                        AND flag = 'False';
                    """
    update_flag_query = """UPDATE staging.mongo_restaurants SET flag = 'True' 
                            WHERE inserted_at = (SELECT max(inserted_at) FROM staging.mongo_restaurants);
                        """
    cursor = conn.cursor()
    cursor.execute(insert_query)
    cursor.execute(update_flag_query)
    conn.commit()
    cursor.close()


def load_orders_dim():
    """ Загрузка таблицы заказов """

    insert_query = """INSERT INTO dds.Dim_Orders (order_id, payed_by_bonuses, cost, payment, bonus_for_visit, final_status)
                        SELECT _id, payed_by_bonuses, cost, payment, bonus_for_visit, final_status
                        FROM staging.mongo_orders
                        WHERE inserted_at = (SELECT max(inserted_at) FROM staging.mongo_orders)
                        AND flag = 'False';
                    """
    cursor = conn.cursor()
    cursor.execute(insert_query)
    conn.commit()
    cursor.close()


def load_delivery_dim():
    """ Загрузка таблицы доставок """

    insert_query = """INSERT INTO dds.Dim_Delivery (delivery_id, delivery_address, delivery_time, order_date_created, rating, tips)
                        SELECT delivery_id, delivery_address, delivery_time, order_date_created, rating, tips
                        FROM staging.api_delivery
                        WHERE inserted_at = (SELECT max(inserted_at) FROM staging.api_delivery)
                        AND flag = 'False';
                    """
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE dds.dim_delivery;")
    cursor.execute(insert_query)
    conn.commit()
    cursor.close()


def load_deliveryman_dim():
    """ Загрузка таблицы доставщиков """

    insert_query = """INSERT INTO dds.Dim_Deliveryman (id, name)
                        SELECT _id, name
                        FROM staging.api_deliveryman
                        WHERE inserted_at = (SELECT max(inserted_at) FROM staging.api_deliveryman)
                        AND flag = 'False';
                    """
    update_flag_query = """UPDATE staging.api_deliveryman SET flag = 'True' 
                            WHERE inserted_at = (SELECT max(inserted_at) FROM staging.api_deliveryman)
                        """
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE dds.dim_deliveryman;")
    cursor.execute(insert_query)
    cursor.execute(update_flag_query)
    conn.commit()
    cursor.close()


default_args = {
    'owner': 'vv',
    'start_date': days_ago(1)
}

with DAG(
        'dds_delta',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    load_clients_dds = PythonOperator(
        task_id='load_clients_dds',
        python_callable=load_clients_dim,
        dag=dag,
        trigger_rule='all_success'
    )

    load_orders_dds = PythonOperator(
        task_id='load_orders_dds',
        python_callable=load_orders_dim,
        dag=dag,
        trigger_rule='all_success'
    )

    load_restaurants_dds = PythonOperator(
        task_id='load_restaurants_dds',
        python_callable=load_restaurants_dim,
        dag=dag,
        trigger_rule='all_success'
    )

    load_delivery_dds = PythonOperator(
        task_id='load_delivery_dds',
        python_callable=load_delivery_dim,
        dag=dag,
        trigger_rule='all_success'
    )

    load_deliveryman_dds = PythonOperator(
        task_id='load_deliveryman_dds',
        python_callable=load_deliveryman_dim,
        dag=dag,
        trigger_rule='all_success'
    )

    load_time_dds = PythonOperator(
        task_id='load_time_dds',
        python_callable=load_time_dim,
        dag=dag,
        trigger_rule='all_success'
    )

    load_fact_dds = PythonOperator(
        task_id='load_fact_dds',
        python_callable=load_fact_table,
        dag=dag,
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

start_load >> [load_clients_dds, load_delivery_dds, load_restaurants_dds, load_orders_dds,
               load_deliveryman_dds] >> load_time_dds >> load_fact_dds >> end_load
