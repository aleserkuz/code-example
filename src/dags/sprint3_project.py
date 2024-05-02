import json
import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'pg_connection'

nickname = Variable.get('nickname')
cohort = Variable.get('cohort')

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


os.chdir('/lessons/downloaded_csv/')


formatter = logging.Formatter('%(asctime)s %(funcName)s():%(lineno)s – %(levelname)s: %(message)s',
                              datefmt='%d/%m/%Y %H:%M:%S')
filehandler = logging.FileHandler('sprint3.log')
filehandler.setFormatter(formatter)
logger = logging.getLogger('sprint3')
logger.setLevel(logging.DEBUG)
logger.addHandler(filehandler)


def generate_report(ti):
    logger.debug('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    logger.debug(f'Response is {response.content}')


def get_report(ti):
    logger.debug('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        logger.debug(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError("Request get_report timed out")

    ti.xcom_push(key='report_id', value=report_id)
    logger.debug(f'Report_id={report_id}')


def upload_general_report(ti):
    report_id = ti.xcom_pull(key='report_id')

    storage_url = 'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT_NUMBER}/{NICKNAME}/project/{REPORT_ID}/{FILE_NAME}'
    personal_storage_url = storage_url.replace("{COHORT_NUMBER}", cohort)
    personal_storage_url = personal_storage_url.replace("{NICKNAME}", nickname)
    personal_storage_url = personal_storage_url.replace("{REPORT_ID}", report_id)

    df_customer_research = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "customer_research.csv"))
    df_customer_research.to_csv("customer_research.csv", sep=',')

    df_user_order_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_order_log.csv"))
    df_user_order_log.to_csv("user_order_log.csv", sep=',')

    df_user_activity_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_activity_log.csv"))
    df_user_activity_log.to_csv("user_activity_log.csv", sep=',')

    df_price_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "price_log.csv"))
    df_price_log.to_csv("price_log.csv", sep=',')

    ti.xcom_push(key='report_id', value=report_id)


def get_increment(date, ti):
    logger.debug('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    logger.debug(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError('Increment is empty. Most probably due to error in API call.')

    ti.xcom_push(key='increment_id', value=increment_id)
    logger.debug(f'increment_id={increment_id}')


def upload_increment(filename, date, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    local_filename = date.replace('-', '') + '_' + filename
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    df.to_csv(local_filename, sep=',')


def upload_increment_to_pg(filename, date, pg_table, pg_schema, ti):
    local_filename = date.replace('-', '') + '_' + filename
    df = pd.read_csv(local_filename, index_col=0)

    if pg_table == 'user_order_log' or pg_table == 'user_activity_log':
        df = df.drop(['id', 'uniq_id'], axis=1)

    if 'status' not in df.columns and pg_table == 'user_order_log':
        df['status'] = 'shipped'

    psql_conn = BaseHook.get_connection(postgres_conn_id)
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    if pg_table == 'customer_research':
        delete_uol = f"DELETE FROM staging.{pg_table} WHERE CAST(date_id AS DATE) = '{date}';"
    else:
        delete_uol = f"DELETE FROM staging.{pg_table} WHERE CAST(date_time AS DATE) = '{date}';"
    cur.execute(delete_uol)

    cols = ','.join(list(df.columns))
    i = 0
    step = int(df.shape[0] / 100)
    while i <= df.shape[0]:
        uol_val = str([tuple(x) for x in df.loc[i:i + step].to_numpy()])[1:-1]
        if (uol_val):
            insert_uol = f"insert into staging.{pg_table} ({cols}) VALUES {uol_val};"
            cur.execute(insert_uol)
            conn.commit()

        i += step+1


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

business_dt = '{{ ds }}'

with DAG(
        'sales_mart',
        default_args=args,
        description='Incremental daily load',
        start_date=datetime.today() - timedelta(days=8),
        schedule_interval='@daily',
        catchup=True,
) as dag:

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    upload_general_report = PythonOperator(
        task_id='upload_general_report',
        python_callable=upload_general_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    with TaskGroup("upload_increments") as upload_increments:
        upload_increment_tasks = list()
        for filename in ['customer_research_inc',
                         'user_order_log_inc',
                         'user_activity_log_inc']:
            upload_increment_tasks.append(PythonOperator(
                task_id=f'upload_increment_{filename}',
                python_callable=upload_increment,
                op_kwargs={
                    'date': business_dt,
                    'filename': f"{filename}.csv"}))

    with TaskGroup("upload_to_pg") as upload_to_pg:
        upload_to_pg_tasks = list()
        for table_name in ['customer_research',
                           'user_order_log',
                           'user_activity_log']:
            upload_to_pg_tasks.append(PythonOperator(
                task_id=f'upload_to_pg_{table_name}',
                python_callable=upload_increment_to_pg,
                op_kwargs={
                    'date': business_dt,
                    'filename': f"{table_name}_inc.csv",
                    'pg_table': table_name,
                    'pg_schema': 'staging'}))

    with TaskGroup("dimensions_load") as dimensions_load:
        dimension_tasks = list()
        for i in ['d_calendar', 'd_city', 'd_item', 'd_customer']:
            dimension_tasks.append(PostgresOperator(
                task_id=f'load_{i}',
                postgres_conn_id=postgres_conn_id,
                sql=f'sql/mart.{i}.sql',
                dag=dag))

    with TaskGroup("facts_load") as facts_load:
        fact_tasks = list()
        for i in ['f_daily_sales', 'f_customer_retention']:
            fact_tasks.append(PostgresOperator(
                task_id=f'load_{i}',
                postgres_conn_id=postgres_conn_id,
                sql=f'sql/mart.{i}.sql',
                dag=dag))

    (
            generate_report
            >> get_report
            >> upload_general_report
            >> get_increment
            >> upload_increments
            >> upload_to_pg
            >> dimensions_load
            >> facts_load
    )
