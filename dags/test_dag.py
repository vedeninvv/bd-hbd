from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "owner": "vv",
    "start_date": days_ago(1)
}

with DAG('graph_dds', schedule_interval=None, tags=['example'], default_args=default_args) as dag:

    matdoc = DummyOperator(
        task_id = 'matdoc',
        dag=dag
    )

    qbew = DummyOperator(
        task_id = 'qbew',
        dag=dag
    )

    mbew = DummyOperator(
        task_id = 'mbew',
        dag=dag
    )   

    ebew = DummyOperator(
        task_id = 'ebew',
        dag=dag
    )

    dt = DummyOperator(
        task_id = 'decision_tree',
        dag=dag
    )   

    terminator = DummyOperator(
        task_id = 'terminator',
        dag=dag
    )

    val_class_sub = DummyOperator(
        task_id = 'val_class_sub',
        dag=dag
    )

    mat_class_val = DummyOperator(
        task_id = 'mat_val_class',
        dag=dag
    )
    upd_mbew = DummyOperator(
        task_id = 'upd_mbew',
        dag=dag
    )

    upd_qbew = DummyOperator(
        task_id = 'upd_qbew',
        dag=dag
    )

    upd_ebew = DummyOperator(
        task_id = 'upd_ebew',
        dag=dag
    )

    
    mbew >> mat_class_val
    matdoc >> [upd_ebew, upd_mbew, upd_qbew]
    ebew >> upd_ebew
    qbew >> upd_qbew
    mat_class_val >> upd_mbew
    dt >> val_class_sub
    terminator >> val_class_sub