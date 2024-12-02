from airflow import DAG
from datetime import datetime
import os 
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow.operators.python import PythonOperator
from pipelines.reddit_pipeline import reddit_pipeline
from utils.constants import *

#Define variable
default_args = {
    'owner': 'EthanDo',
    'start_date': datetime(2024, 11, 8), 
}
previous_task = None
# Define the DAG
dag = DAG(
    dag_id='reddit_crawler_job',
    default_args=default_args,
    schedule_interval='0 */3 * * *',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline', 'crawl', 'canada']
)

# Create tasks for each subreddit and chain them sequentially
for subreddit in SUBREDDIT_LISTS:
    extract_task = PythonOperator(
        task_id=f'reddit_extraction_{subreddit}',
        python_callable=reddit_pipeline,
        op_kwargs={
            'subreddit': subreddit,
            'limit': None
        },
        dag=dag
    )
    
    # Set the task to run after the previous task, creating a sequential chain
    if previous_task:
        previous_task >> extract_task
    previous_task = extract_task  # Update the previous_task variable
