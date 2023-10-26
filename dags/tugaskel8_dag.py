from datetime import datetime,timedelta

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.hooks.postgres_hook import PostgresHook

import pandas as pd
import requests
import json

default_args = {
    "owner": "kel8",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

# EXTRACT DATA FROM OPENAQ
def extract_data(**context):
    url = "https://api.openaq.org/v2/locations?limit=100&page=1&offset=0&sort=desc&radius=1000&country=ID&order_by=lastUpdated&dump_raw=false"

    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)
    response_content = response.content.decode('utf-8')

    return context['ti'].xcom_push(key='url_response',value=response_content)

def transform_data(**context):
    data = context['ti'].xcom_pull(key='url_response')
    tes_data = json.loads(data)
    results = tes_data['results']
    # Filter data yang namenya Jakarta Center
    filtered_results = [x for x in results if 'Jakarta' in x['name']]
    # Masukkan data resultsnya ke dalam pandas dataframe
    df = pd.DataFrame(filtered_results)
    df['parameters'] = list(map(lambda x: json.dumps(x), df['parameters']))
    df['coordinates'] = list(map(lambda x: json.dumps(x), df['coordinates']))
    df['bounds'] = list(map(lambda x: json.dumps(x), df['bounds']))
    df['manufacturers'] = list(map(lambda x: json.dumps(x), df['manufacturers']))
    return context['ti'].xcom_push(key='df',value=df)

def load_data(**context):
    final_df = context['ti'].xcom_pull(key='df')
    # Buat koneksi ke database
    hook = PostgresHook(postgres_conn_id='db_polusiudara')
    
    final_df.to_sql('hands_on_final', hook.get_sqlalchemy_engine(), index=False, if_exists='replace')
    return 'Sukses Memasukkan Data'

etl_dag = DAG(
    dag_id="etl_dag",
    default_args=default_args,
    start_date=datetime(2023,10,24),
    catchup=False,
    schedule_interval= "0 4 * * *"
)

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=etl_dag
)
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=etl_dag
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=etl_dag
)

extract_data_task >> transform_data_task >> load_data_task 