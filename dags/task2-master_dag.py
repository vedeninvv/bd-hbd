from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'vv'
}

with DAG(
        'task2_master_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['task2'],
) as dag:
    file_to_stg = TriggerDagRunOperator(
        task_id='file_to_stg',
        trigger_dag_id='task2_file_to_stg',
        wait_for_completion=True,
        poke_interval=1,
        dag=dag
    )

    stg_to_dds = TriggerDagRunOperator(
        task_id='stg_to_dds',
        trigger_dag_id='task2_stg_to_dds',
        wait_for_completion=True,
        poke_interval=1,
        dag=dag
    )

    dds_to_cdm = TriggerDagRunOperator(
        task_id='dds_to_cdm',
        trigger_dag_id='task2_dds_to_cdm',
        wait_for_completion=True,
        poke_interval=1,
        dag=dag
    )

    start_load = DummyOperator(
        task_id='start_load',
        dag=dag
    )

    start_load >> file_to_stg >> stg_to_dds >> dds_to_cdm
