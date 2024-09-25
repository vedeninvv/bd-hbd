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

""" В процессе разработки промежуток времени для измерений данных в витрине ставил вручную удобный для отладки.
    В итоговой версии видимо необходимо месяц заменить на `EXTRACT(MONTH FROM now())::SMALLINT -1`,
                                        год на `EXTRACT(YEAR FROM now())::SMALLINT` (также убрать условие WHERE)
    для того чтобы отслеживать информацию по доставщикам за последний месяц
"""

conn = psycopg2.connect(**conn_params)


def load_cdm_on_date():
    """ Загружаем данные из dds в витрину """

    insert_query = """INSERT INTO cdm.deliveryman_income (
                            deliveryman_id, deliveryman_name, year, month, orders_amount, orders_total_cost, rating, company_commission, deliveryman_order_income, tips
                        )
                        SELECT
                            d.id AS deliveryman_id,
                            d.name AS deliveryman_name,
                            2024 AS year,
                            12 AS month,
                            COUNT(ft.id) AS orders_amount,
                            SUM(o.cost::NUMERIC) AS orders_total_cost,
                            AVG(dv.rating::NUMERIC) AS rating,
                            SUM(o.cost::NUMERIC) * 0.5 AS company_commission,
                            CASE
                                WHEN AVG(dv.rating::NUMERIC) < 8 THEN
                                    CASE
                                        WHEN SUM(o.cost::NUMERIC) * 0.05 < 400 THEN 400
                                        ELSE SUM(o.cost::NUMERIC) * 0.05
                                    END
                                WHEN AVG(dv.rating::NUMERIC) >= 10 THEN
                                    CASE
                                        WHEN SUM(o.cost::NUMERIC) * 0.1 > 1000 THEN 1000
                                        ELSE SUM(o.cost::NUMERIC) * 0.1
                                    END
                                ELSE
                                    SUM(o.cost::NUMERIC) * 0.05
                            END AS deliveryman_order_income,
                            SUM(dv.tips::NUMERIC) AS tips
                        FROM
                            dds.Fact_Table ft
                            JOIN dds.Dim_Delivery dv ON ft.delivery_id = dv.delivery_id
                            JOIN dds.Dim_Deliveryman d ON ft.deliveryman_id = d.id
                            JOIN dds.Dim_Orders o ON ft.order_id = o.order_id
                        WHERE
                            EXTRACT(YEAR FROM dv.delivery_time) = 2024
                            AND EXTRACT(MONTH FROM dv.delivery_time) = 12
                        GROUP BY
                            d.id, d.name;
                    """
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE cdm.deliveryman_income;")
    cursor.execute(insert_query)
    conn.commit()
    cursor.close()


default_args = {
    'owner': 'vv',
    'start_date': days_ago(1)
}

with DAG(
        'load_cdm',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    cdm = PythonOperator(
        task_id='cdm',
        python_callable=load_cdm_on_date,
        dag=dag,
        trigger_rule='all_success'
    )

    start_load = DummyOperator(
        task_id='start_load',
        dag=dag
    )

start_load >> cdm
