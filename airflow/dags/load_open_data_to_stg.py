import json
import io
import zipfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

TABLE_NAME = 'stg.tbl_transport'

POSTGRES_CONN_ID = "open_data_dwh_postgresql"
HTTP_CONN_ID = "open_data_http"

def process_load_open_data_to_stg(**kwargs):    
    transports = kwargs['ti'].xcom_pull(task_ids='request_open_data')
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    for transport in transports:
        tablename = TABLE_NAME
        int_id = transport['ID']
        year = transport['Year']
        transport_type = transport['TransportType']
        passenger_traffic = transport['PassengerTraffic']
        global_id = transport['global_id']
        sql = f"INSERT INTO {tablename} (int_id, year, transport_type, passenger_traffic, global_id) VALUES({int_id}, {year}, '{transport_type}', {passenger_traffic}, {global_id});"
        pg.run(sql, autocommit=True)

    return transports

def exctract_json_from_zip(response):    
    content = response.content
    print(type(content))
    print(content)
    with zipfile.ZipFile(io.BytesIO(content), mode="r") as archive:
        text = archive.read(archive.namelist()[0]).decode(encoding='windows-1251')
        open_data = json.loads(text)
        print(open_data)
    return open_data
    
with DAG(
        dag_id='load_open_data_to_stg',
        default_args=default_args,
        description='Load open data by transport from Moscow government',
        catchup=False,
        start_date=datetime(2024, 3, 17),
        schedule_interval=None
) as dag:
    request_open_data = HttpOperator(
        task_id='request_open_data',
        http_conn_id=HTTP_CONN_ID,
        endpoint='/odata/export/catalog?idFile=178966',
        headers={"Content-Type": "application/zip"},
        log_response=True,
        response_filter=exctract_json_from_zip,
        method='GET'
    )
    load_open_data_to_stg = PythonOperator(
        task_id='load_open_data_to_stg',
        python_callable=process_load_open_data_to_stg
    )

request_open_data >> load_open_data_to_stg
