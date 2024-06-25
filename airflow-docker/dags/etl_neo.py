import requests as req
import boto3
from io import StringIO
from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

def get_nasa_data(ti) -> None:
    today = date.today()
    startDate = today.strftime("%Y-%m-%d")
    endDate = today + timedelta(days=7)
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={startDate}&end_date={endDate}&api_key={os.getenv('API_KEY')}"
    response = req.get(url)
    response.raise_for_status()
    data = response.json()
    ti.xcom_push(key='fetch_nasa_data', value=data)

def transform_data(ti) -> None:
    response = ti.xcom_pull(key='fetch_nasa_data', task_ids='fetch_nasa_data')
    near_earth_objects = response['near_earth_objects']
    asteroid_data = []
    for date in near_earth_objects:
        for asteroid in near_earth_objects[date]:
            asteroid_data.append({
                'name': asteroid['name'],
                'absolute_magnitude': asteroid['absolute_magnitude_h'],
                'estimated_diameter': asteroid['estimated_diameter']['kilometers']['estimated_diameter_max'],
                'close_approach_date': asteroid['close_approach_data'][0]['close_approach_date'],
                'orbiting_body': asteroid['close_approach_data'][0]['orbiting_body']
            })
    df = pd.DataFrame(asteroid_data)
    ti.xcom_push(key='transform_nasa_data', value=df.to_json(orient='split'))

def load_data_to_aws(ti):
    response = ti.xcom_pull(key='transform_nasa_data', task_ids='transform_nasa_data')
    df = pd.read_json(response, orient='split')

    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )

    s3_res = session.resource('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    bucket = 'nasatestebucket'
    s3_object_name = 'df.csv'
    s3_res.Object(bucket, s3_object_name).put(Body=csv_buffer.getvalue())

default_args = {
    'owner': 'test',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id='nasa_neo_data_to_s3',
    description='Fetch Near Earth Object data from NASA and save to S3',
    start_date=datetime(2024, 6, 23),
    schedule_interval=timedelta(days=1)
) as dag:

    task1_neo = PythonOperator(
        task_id='fetch_nasa_data',
        python_callable=get_nasa_data
    )

    task2_neo = PythonOperator(
        task_id='transform_nasa_data',
        python_callable=transform_data,
        retries=3  # Número de tentativas para a task2
    )

    task3_neo = PythonOperator(
        task_id='load_data_to_aws',
        python_callable=load_data_to_aws
    )

    task1_neo >> task2_neo >> task3_neo
