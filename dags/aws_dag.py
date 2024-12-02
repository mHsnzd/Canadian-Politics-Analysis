
from airflow import DAG
from datetime import datetime, time
import os 
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow.operators.python import PythonOperator
from utils.constants import *
from pipelines.aws_s3_pipeline import upload_s3_pipeline


#Define variable
default_args = {
    'owner': 'EthanDo',
    'start_date': datetime(2024, 11, 8), 
}

dt = datetime.combine(datetime.today(), time.min)

dag = DAG(
    dag_id='reddit_etl_aws',
    default_args=default_args,
    schedule_interval='30 0 * * *',  # Runs daily at 12:30 AM
    catchup=False,
    tags=['reddit', 'etl', 'pipeline', 'crawl', 'canada']
)

etl_task = PythonOperator(
    task_id=f'reddit_etl_from_database',
    python_callable=upload_s3_pipeline,
    op_kwargs={
        'dt': dt
    },
    dag=dag
)
