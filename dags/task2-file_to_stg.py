import pendulum
import csv
from airflow.decorators import dag, task
import vertica_python

conn_info = {
    'host': '172.18.208.1',
    'port': '5433',
    'user': 'dbadmin',
    'password': '',
    'database': 'VMart',
    'schema': 'public',
}
file_folder = '/mnt/c/Users/vedev/Desktop/BD/airflow_project/task1/dags/files'

default_args = {
    'owner': 'vv',
}

@dag(
    schedule_interval=None,
    start_date=pendulum.now(),
    catchup=False,
    tags=['task2'],
    is_paused_upon_creation=False,
    default_args=default_args
)
def task2_file_to_stg():
    def load_csv_to_vertica(file_path, table_name):
        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                columns = next(reader)
                query = f"INSERT INTO public.{table_name} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})"
                for data in reader:
                    cursor.execute(query, data)
            connection.commit()

    def truncate(table_name):
        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            truncate_query = f"TRUNCATE TABLE public.{table_name}"
            cursor.execute(truncate_query)
            connection.commit()

    @task
    def load_peoples():
        truncate("peoples")
        load_csv_to_vertica(file_folder + "/peoples.csv", "peoples")

    @task
    def load_chats():
        truncate("chats")
        load_csv_to_vertica(file_folder + "/chats.csv", "chats")

    @task
    def load_groups():
        truncate("groups")
        load_csv_to_vertica(file_folder + "/groups.csv", "groups")

    load_peoples() >> load_chats() >> load_groups()


file_to_stg = task2_file_to_stg()
