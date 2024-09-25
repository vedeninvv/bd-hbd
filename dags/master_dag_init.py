from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'vv',
    'start_date': days_ago(1)
}

with DAG(
        'master_dag_init',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    api_to_stg = TriggerDagRunOperator(
        task_id='api_to_stg',
        trigger_dag_id='load_api_to_stg_init',
        wait_for_completion=True,
        dag=dag
    )

    mongo_to_stg = TriggerDagRunOperator(
        task_id='mongo_to_stg',
        trigger_dag_id='mongo_to_stg_init',
        wait_for_completion=True,
        dag=dag
    )

    postgre_to_stg = TriggerDagRunOperator(
        task_id='postgre_to_stg',
        trigger_dag_id='postgres_to_stg_init',
        wait_for_completion=True,
        dag=dag
    )

    stg_to_dds = TriggerDagRunOperator(
        task_id='stg_to_dds',
        trigger_dag_id='dds_init',
        wait_for_completion=True,
        dag=dag,
        trigger_rule='all_success'
    )

    dds_to_cdm = TriggerDagRunOperator(
        task_id='dds_to_cdm',
        trigger_dag_id='load_cdm',
        wait_for_completion=True,
        dag=dag
    )

    start_load = DummyOperator(
        task_id='start_load',
        dag=dag
    )

    start_load >> [api_to_stg, mongo_to_stg, postgre_to_stg] >> stg_to_dds >> dds_to_cdm
