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
    import requests as req
    url = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
    query = """
        SELECT 
            pl_name, 
            hostname, 
            discoverymethod, 
            disc_year, 
            pl_orbper, 
            pl_rade 
        FROM 
            ps 
        WHERE 
            disc_year > 2010
            """
    params = {
        "query": query,
        "format": "json"
        }
    response = req.get(url, params=params)
    data = response.json()
    ti.xcom_push(key='fetch_nasa_exo_data', value=data)
    
def transform_exo_data(ti) -> None:
    response = ti.xcom_pull(key='fetch_nasa_exo_data', task_ids='fetch_nasa_exo_data')
    exo_planets = []
    for planets in response:
        exo_planets.append({
                'name': planets['pl_name'],
                'star': planets['hostname'],
                'discovery_method': planets['discoverymethod'],
                'disc_year': planets['disc_year'],
                'pl_orbper': planets['pl_orbper'],
                'pl_rade': planets['pl_rade']
            })
    df = pd.DataFrame(exo_planets)
    ti.xcom_push(key='transform_nasa_exo_data', value=df.to_json(orient='split'))

def load_exo_data_to_aws(ti):
    response = ti.xcom_pull(key='transform_nasa_exo_data', task_ids='transform_nasa_exo_data')
    df = pd.read_json(response, orient='split')

    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )

    s3_res = session.resource('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    bucket = 'nasatestebucket'
    s3_object_name = 'df_exo.csv'
    s3_res.Object(bucket, s3_object_name).put(Body=csv_buffer.getvalue())
    
default_args = {
    'owner': 'test',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id='nasa_exo_data_to_s3',
    description='Fetch Exo planets data from NASA and save to S3',
    start_date=datetime(2024, 6, 23),
    schedule_interval=timedelta(days=1)
) as dag:

    task1 = PythonOperator(
        task_id='fetch_nasa_exo_data',
        python_callable=get_nasa_data
    )

    task2 = PythonOperator(
        task_id='transform_nasa_exo_data',
        python_callable=transform_exo_data,
        retries=3  # Número de tentativas para a task2
    )

    task3 = PythonOperator(
        task_id='load_exo_data_to_aws',
        python_callable=load_exo_data_to_aws
    )

    task1 >> task2 >> task3
