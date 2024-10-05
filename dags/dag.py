from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from tasks import connect, process, upload
import pendulum


with DAG(
    dag_id='Molecule_processing_DAG',
    start_date=pendulum.today(),
    schedule='0 8 * * *',
    tags=['python_school', 'molecules']
) as dag:
    start_op = EmptyOperator(
        task_id='start'
    )
    connect_to_db_op = PythonOperator(
        task_id='connect_to_db',
        python_callable=connect
    )
    process_data_op = PythonOperator(
        task_id='process_data',
        python_callable=process
    )
    upload_data_op = PythonOperator(
        task_id='upload_data_to_s3',
        python_callable=upload
    )
    finish_op = EmptyOperator(
        task_id='finish'
    )

    start_op >> connect_to_db_op >> process_data_op 
    process_data_op >> upload_data_op >> finish_op