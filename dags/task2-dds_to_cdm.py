import pendulum
from airflow.decorators import dag, task
import vertica_python

conn_info = {
    'host': '172.18.208.1',
    'port': '5433',
    'user': 'dbadmin',
    'password': '',
    'database': 'VMart',
}

default_args = {
    'owner': 'vv',
}

def truncate(connection, table_name):
    cursor = connection.cursor()
    truncate_query = f"TRUNCATE TABLE public.{table_name}"
    cursor.execute(truncate_query)
    connection.commit()


@dag(
    schedule_interval=None,
    start_date=pendulum.now(),
    catchup=False,
    tags=['task2'],
    is_paused_upon_creation=False,
    default_args=default_args
)
def task2_dds_to_cdm():
    @task
    def get_user_age_groups():
        query = """        
            -- Определяем 5 групп с наибольшим количеством сообщений
            WITH top_groups AS (
                SELECT g.hk_group_id
                FROM L_groups g
                    JOIN h_chats c ON g.hk_msg_id = c.hk_msg_id
                GROUP BY g.hk_group_id
                ORDER BY COUNT(c.hk_msg_id) DESC
                LIMIT 5
            ),

            -- Определяем сообщения из топ-5 групп
            messages_in_top_groups AS (
                SELECT l.hk_msg_id
                FROM L_groups l
                JOIN top_groups tg ON l.hk_group_id = tg.hk_group_id
            ),

            -- Находим пользователей, которые писали сообщения в этих группах
            users_in_top_groups AS (
                SELECT DISTINCT l.hk_person_id
                FROM L_person_chat l
                    JOIN messages_in_top_groups mtg ON l.hk_msg_id = mtg.hk_msg_id
            ),

            -- Рассчитываем возрастные группы пользователей
            age_groups AS (
                SELECT
                    CASE
                        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM s.date_of_birthday) BETWEEN 0 AND 17 THEN '0-17'
                        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM s.date_of_birthday) BETWEEN 18 AND 25 THEN '18-25'
                        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM s.date_of_birthday) BETWEEN 26 AND 35 THEN '26-35'
                        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM s.date_of_birthday) BETWEEN 36 AND 45 THEN '36-45'
                        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM s.date_of_birthday) BETWEEN 46 AND 60 THEN '46-60'
                        ELSE '61+'
                    END AS age_group,
                    COUNT(DISTINCT u.hk_person_id) AS user_count
                FROM users_in_top_groups u
                    JOIN s_person_info s ON u.hk_person_id = s.hk_person_id
                GROUP BY age_group
            )

            -- Выводим результаты
            SELECT age_group, user_count
            FROM age_groups
            ORDER BY age_group;
        """
        with vertica_python.connect(**conn_info) as connection:
            truncate(connection, 'user_age_groups')
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            print(f"RESULT -> {result}")
            return result

    @task
    def populate_view_table(data):
        insert_query = """
            INSERT INTO public.user_age_groups (age_group, user_count) VALUES (:age_group, :user_count)
        """
        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            for row in data:
                cursor.execute(insert_query, {'age_group': row[0], 'user_count': row[1]})
            connection.commit()

    user_age_groups = get_user_age_groups()

    populate_view_table(user_age_groups)


migraion = task2_dds_to_cdm()
