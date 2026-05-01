import os
import random
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 10,
    'retry_delay': timedelta(seconds=10),
}

def get_project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def generate_raw_features(ti) -> None:
    data = [{
        'user_id': random.randint(1000, 1050),
        'session_duration_sec': random.randint(5, 1200),
        'clicks': random.choices([0, 1, 5, 10, 50], weights=[20, 30, 30, 15, 5])[0] 
    } for _ in range(10000)]
        
    df = pd.DataFrame(data)
    target_dir = os.path.join(get_project_root(), 'airflow_output')
    os.makedirs(target_dir, exist_ok=True)
    file_path = os.path.join(target_dir, 'raw_features.csv')
    df.to_csv(file_path, index=False)
    ti.xcom_push(key='dataset_path', value=file_path)

def process_with_flaky_api(ti) -> None:
    file_path = ti.xcom_pull(key='dataset_path', task_ids='generate_data_task')
    df = pd.read_csv(file_path)
    
    if random.random() < 0.7:
        raise ValueError("Simulated API Error! Task failed, awaiting retry...")
    
    df['is_active'] = df['clicks'] > 2
    final_path = os.path.join(os.path.dirname(file_path), 'enriched_features.csv')
    df.to_csv(final_path, index=False)

with DAG(
    'ml_feature_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['university_project', 'ml_engineering']
) as dag:

    task_generate_data = PythonOperator(
        task_id='generate_data_task',
        python_callable=generate_raw_features,
    )

    task_process_data = PythonOperator(
        task_id='process_data_task',
        python_callable=process_with_flaky_api,
    )

    task_spark_aggregation = SparkSubmitOperator(
        task_id='spark_aggregation_task',
        conn_id='spark_default',
        spark_binary='/home/airflow/.local/bin/spark-submit',
        application='/opt/airflow/spark_jobs/aggregate_features.py',
        name='ML_Feature_Aggregation_Job',

        
        application_args=[
            '/opt/airflow/airflow_output/enriched_features.csv'
        ],
        conf={
            'spark.executor.instances': '3',
            'spark.executor.cores': '1',
            'spark.executor.memory': '1g'
        },
        verbose=True
    )

    task_generate_data >> task_process_data >> task_spark_aggregation