from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, time
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import OUTPUT_PATH
from etls.aws_etl import upload_folder_s3

# Define default arguments for the DAG
default_args = {
    'owner': 'EthanDo',
    'start_date': datetime(2024, 11, 20),  # Start date for the DAG
}

dt = datetime.combine(datetime.today(), time.min)

dt = datetime(2024,11,15)
# Define the DAG
dag = DAG(
    dag_id='reddit_etl_transform',
    default_args=default_args,
    schedule_interval='30 1 * * *',  # Runs daily at 12:30 AM
    catchup=False,
    tags=['reddit', 'etl', 'pipeline', 'transform', 'canada']
)

# Define the task using SparkSubmitOperator
clean_task = SparkSubmitOperator(
    task_id='reddit_transform_data',
    application='transform/clean_data.py',   
    application_args=[dt.strftime('%Y%m%d'), OUTPUT_PATH],  
    conn_id='spark_default',
    verbose=True,
    dag=dag
)

sentiment_analysis = SparkSubmitOperator(
    task_id='sentiment_analysis',
    application='transform/sentiment_analysis.py', 
    application_args=[dt.strftime('%Y%m%d'), OUTPUT_PATH],  
    conn_id='spark_default',
    verbose=True,
    dag=dag
)

emotion_classification = SparkSubmitOperator(
    task_id='emotion_classification',
    application='transform/emotion_classification.py',   
    application_args=[dt.strftime('%Y%m%d'), OUTPUT_PATH],  
    conn_id='spark_default',
    verbose=True,
    dag=dag
)

hate_detector = SparkSubmitOperator(
    task_id='hate_detector',
    application='transform/hate_detector.py', 
    application_args=[dt.strftime('%Y%m%d'), OUTPUT_PATH],  
    conn_id='spark_default',
    verbose=True,
    dag=dag
)
upload_task = PythonOperator(
    task_id=f'reddit_upload_transformed_data',
    python_callable=upload_folder_s3,
    op_kwargs={
        'dt': dt
    },
    dag=dag
)

clean_task >> sentiment_analysis >> emotion_classification >> hate_detector >> upload_task