from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import pendulum
import boto3
import os
import vertica_python
import logging

SOURCE_FOLDER = '/data'
SCHEMA_NAME = 'STV2023111337__STAGING'
ESCAPE = '\\'

def add_escape(file_name: str, columns: dict, delimiter:str=',',escape:str=ESCAPE):
    file_path = f"{SOURCE_FOLDER}/{file_name}"
    df = pd.read_csv(file_path, delimiter=delimiter)
    for index, rows in df.iterrows():
        for column in columns:
            s = str(df.iloc[index][column])
            df.at[index, column] = s.replace(delimiter,escape+delimiter)
    
    os.rename(file_path, file_name[:-4]+'_old.csv')
    df.to_csv(file_path, index=False)

def load_data(key: str):
    table_name = key[:-4]
    list_columns = pd.read_csv(f'{SOURCE_FOLDER}/{key}', nrows=0).columns.tolist()
    if key == 'dialogs.csv':
        list_columns[list_columns.index('message_group')] = 'message_group_csv filler numeric,message_group as message_group_csv::int'
    if key == 'group_log.csv':   
        list_columns[list_columns.index('user_id_from')] = 'user_id_from_csv filler numeric,user_id_from as user_id_from_csv::int'
    str_columns = ','.join(list_columns)
    sql_copy = f"""
    copy {SCHEMA_NAME}.{table_name} ({str_columns})
    from local '{SOURCE_FOLDER}/{key}'
    delimiter ','
    escape '{ESCAPE}'
    ENCLOSED BY '"'
    ENFORCELENGTH;
    """

    logging.info(sql_copy)

    conn_info = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2023111337',       
             'password': '79IcbEuKZysT3Ax',
             'database': 'dwh',
             # Вначале он нам понадобится, а дальше — решите позже сами
            'autocommit': True
    }

    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(sql_copy) 


@task
def get_data(key: str, src_folder: str):
  
    AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

    session = boto3.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key=key,
        Filename=src_folder+'/'+key
    )
    if key == 'dialogs.csv':
        add_escape(
            file_name=key, 
            columns=['message']
        )
    
    load_data(key)

@dag(
    start_date = pendulum.datetime(2023, 12, 1, 0, 0, 0, tz='Europe/Moscow'),
    schedule_interval=None
)
def sprint6_dag_load_data():

    # Инициализируем таски получения данных
    files = ['users.csv', 'groups.csv', 'dialogs.csv', 'group_log.csv']
    task_dict_get = {}
    for bucket in files:
        #task_dict_get[bucket] = get_data.override(task_id=f'fetch_{bucket}')(bucket, SOURCE_FOLDER)
        task_dict_get[bucket] = get_data(bucket, SOURCE_FOLDER, task_id=f'fetch_{bucket}')
    #Если давать имена таскам,то они почему-то не будут отображаться в grid
    #Преподаватель не дал ответа почему так
    # for file in files:
    #     task_dict_get[file] = PythonOperator(
    #         task_id=f'fetch_{file}',
    #         python_callable=get_data,
    #         op_kwargs={'key': file,'src_folder': SOURCE_FOLDER} 
    #     )

    bash_command_tmpl=bash_command_tmpl = """
    head {{ params.files }}
    """
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': " ".join(f'/data/{f}' for f in files)}
    )

    [task_dict_get['users.csv'], task_dict_get['groups.csv'], task_dict_get['dialogs.csv']]## >> print_10_lines_of_each   

dag_get_data = sprint6_dag_load_data()     
dag_get_data