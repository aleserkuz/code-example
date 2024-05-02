import datetime
import json
import os
import time

import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python import PythonOperator

api_conn = HttpHook.get_connection('http_conn_id')
api_endpoint = api_conn.host
api_key = api_conn.extra_dejson.get('api_key')

nickname = 'aleserkuz'
cohort = '16'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

psql_conn = BaseHook.get_connection('pg_connection')

conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()

os.chdir('/migrations/')


def create_files_request(ti, api_endpoint, headers):
    method_url = '/generate_report'
    print('URL IS ' + api_endpoint + method_url)
    r = requests.post(api_endpoint + method_url, headers=headers)
    response_dict = json.loads(r.content)
    ti.xcom_push(key='task_id', value=response_dict['task_id'])
    print(f"task_id is {response_dict['task_id']}")
    return response_dict['task_id']


def check_report(ti, api_endpoint, headers):
    task_ids = ti.xcom_pull(key='task_id', task_ids=['create_files_request'])
    task_id = task_ids[0]
    method_url = '/get_report'
    payload = {'task_id': task_id}

    for i in range(4):
        time.sleep(120)
        r = requests.get(api_endpoint + method_url, params=payload, headers=headers)
        print(r)
        response_dict = json.loads(r.content)
        print(i, response_dict['status'])
        if response_dict['status'] == 'SUCCESS':
            report_id = response_dict['data']['report_id']
            break

    ti.xcom_push(key='report_id', value=report_id)
    print(f"report_id is {report_id}")
    return report_id


def create_db_tables(ti):
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    try:
        sqlfile = open('staging_ddl.sql', 'r')
        cur.execute(sqlfile.read())
        conn.commit()
        sqlfile.close()
    except Exception as ex:
        print(type(ex).__name__)
        print("I can't execute staging_ddl.sql script!")

    try:
        sqlfile = open('mart_ddl.sql', 'r')
        cur.execute(sqlfile.read())
        conn.commit()
        sqlfile.close()
    except Exception as ex:
        print(type(ex).__name__)
        print("I can't execute mart_ddl.sql script!")

    cur.close()
    conn.close()

    return 200


def upload_from_s3_to_pg(ti, nickname, cohort):
    report_ids = ti.xcom_pull(key='report_id', task_ids=['check_report'])
    report_id = report_ids[0]

    storage_url = 'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT_NUMBER}/{NICKNAME}/project/{REPORT_ID}/{FILE_NAME}'
    personal_storage_url = storage_url.replace("{COHORT_NUMBER}", cohort)
    personal_storage_url = personal_storage_url.replace("{NICKNAME}", nickname)
    personal_storage_url = personal_storage_url.replace("{REPORT_ID}", report_id)

    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    df_customer_research = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "customer_research.csv"))
    df_customer_research.reset_index(drop=True, inplace=True)
    insert_cr = "insert into staging.customer_research (date_id,category_id,geo_id,sales_qty,sales_amt) VALUES {cr_val};"
    i = 0
    step = int(df_customer_research.shape[0] / 100)
    while i <= df_customer_research.shape[0]:
        print('df_customer_research', i, end='\r')

        cr_val = str([tuple(x) for x in df_customer_research.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_cr.replace('{cr_val}', cr_val))
        conn.commit()

        i += step+1

    df_order_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_order_log.csv"))
    df_order_log.reset_index(drop=True, inplace=True)
    insert_uol = "insert into staging.user_order_log (date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount) VALUES {uol_val};"
    i = 0
    step = int(df_order_log.shape[0] / 100)
    while i <= df_order_log.shape[0]:
        print('df_order_log', i, end='\r')

        uol_val = str([tuple(x) for x in df_order_log.drop(columns=['id', 'uniq_id'], axis=1).loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_uol.replace('{uol_val}', uol_val))
        conn.commit()

        i += step+1

    print(personal_storage_url.replace("{FILE_NAME}", "user_activity_log.csv"))
    df_activity_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_activity_log.csv"))
    df_activity_log.reset_index(drop=True, inplace=True)
    insert_ual = "insert into staging.user_activity_log (date_time, action_id, customer_id, quantity) VALUES {ual_val};"
    i = 0
    step = int(df_activity_log.shape[0] / 100)
    while i <= df_activity_log.shape[0]:
        print('df_activity_log', i, end='\r')

        if df_activity_log.drop(columns=['id', 'uniq_id'], axis=1).loc[i:i + step].shape[0] > 0:
            ual_val = str([tuple(x) for x in df_activity_log.drop(columns=['id', 'uniq_id'], axis=1).loc[i:i + step].to_numpy()])[1:-1]
            cur.execute(insert_ual.replace('{ual_val}', ual_val))
            conn.commit()

        i += step+1

    df_price_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "price_log.csv"))
    df_price_log.reset_index(drop=True, inplace=True)
    insert_ual = "insert into staging.price_log (item_name, price) VALUES {ual_val};"
    i = 0
    step = int(df_price_log.shape[0] / 100)
    while i <= df_price_log.shape[0]:
        print('df_price_log', i, end='\r')

        if df_price_log.loc[i:i + step].shape[0] > 0:
            ual_val = str([tuple(x) for x in df_price_log.loc[i:i + step].to_numpy()])[1:-1]
            cur.execute(insert_ual.replace('{ual_val}', ual_val))
            conn.commit()

        i += step+1

    cur.close()
    conn.close()

    return 200


def update_mart_d_tables(ti):
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    try:
        sqlfile = open('d_calendar.sql', 'r')
        cur.execute(sqlfile.read())
        conn.commit()
        sqlfile.close()
    except Exception as ex:
        print(type(ex).__name__)
        print("I can't execute d_calendar.sql script!")

    try:
        sqlfile = open('d_customer.sql', 'r')
        cur.execute(sqlfile.read())
        conn.commit()
        sqlfile.close()
    except Exception as ex:
        print(type(ex).__name__)
        print("I can't execute d_customer.sql script!")

    try:
        sqlfile = open('d_item.sql', 'r')
        cur.execute(sqlfile.read())
        conn.commit()
        sqlfile.close()
    except Exception as ex:
        print(type(ex).__name__)
        print("I can't execute d_item.sql script!")

    cur.close()
    conn.close()

    return 200


def update_mart_f_tables(ti):
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    try:
        sqlfile = open('f_activity.sql', 'r')
        cur.execute(sqlfile.read())
        conn.commit()
        sqlfile.close()
    except Exception as ex:
        print(type(ex).__name__)
        print("I can't execute f_activity.sql script!")

    try:
        sqlfile = open('f_daily_sales.sql', 'r')
        cur.execute(sqlfile.read())
        conn.commit()
        sqlfile.close()
    except Exception as ex:
        print(type(ex).__name__)
        print("I can't execute f_daily_sales.sql script!")

    try:
        sqlfile = open('f_customer_retention.sql', 'r')
        cur.execute(sqlfile.read())
        conn.commit()
        sqlfile.close()
    except Exception as ex:
        print(type(ex).__name__)
        print("I can't execute f_customer_retention.sql script!")

    cur.close()
    conn.close()

    return 200


def migrate_user_order_log(ti):
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    try:
        sqlfile = open('f_migrate_user_order_log.sql', 'r')
        cur.execute(sqlfile.read())
        conn.commit()
        sqlfile.close()
    except Exception as ex:
        print(type(ex).__name__)
        print("I can't execute f_migrate_user_order_log.sql script!")


business_dt = '{{ ds }}'

dag = DAG(
    dag_id='sprint3_initial',
    schedule_interval=None,
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)


t_file_request = PythonOperator(task_id='create_files_request',
                                python_callable=create_files_request,
                                op_kwargs={'api_endpoint': api_endpoint,
                                           'headers': headers
                                           },
                                dag=dag)

t_check_report = PythonOperator(task_id='check_report',
                                python_callable=check_report,
                                op_kwargs={'api_endpoint': api_endpoint,
                                           'headers': headers
                                           },
                                dag=dag)

t_create_db_tables = PythonOperator(task_id='create_db_tables',
                                    python_callable=create_db_tables,
                                    dag=dag)

t_upload_from_s3_to_pg = PythonOperator(task_id='upload_from_s3_to_pg',
                                        python_callable=upload_from_s3_to_pg,
                                        op_kwargs={'nickname': nickname,
                                                   'cohort': cohort
                                                   },
                                        dag=dag)

t_update_mart_d_tables = PythonOperator(task_id='update_mart_d_tables',
                                        python_callable=update_mart_d_tables,
                                        dag=dag)

t_update_mart_f_tables = PythonOperator(task_id='update_mart_f_tables',
                                        python_callable=update_mart_f_tables,
                                        dag=dag)

t_migrate_user_order_log = PythonOperator(task_id='migrate_user_order_log',
                                          python_callable=migrate_user_order_log,
                                          dag=dag)

(
    t_file_request
    >> t_check_report
    >> t_create_db_tables
    >> t_upload_from_s3_to_pg
    >> t_update_mart_d_tables
    >> t_update_mart_f_tables
    >> t_migrate_user_order_log
)
