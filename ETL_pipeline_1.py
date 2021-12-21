from datetime import timedelta,datetime
from io import StringIO
from pandas._config import config
import boto3
from airflow import DAG
from airflow.utils.decorators import apply_defaults
from airflow.operators.python import PythonOperator,BranchPythonOperator
import airflow.utils.dates
from airflow.models import BaseOperator
from sqlalchemy import create_engine
import json
from pandas import json_normalize
from sqlalchemy import event
# from sqlalchemy.dialects import registry
from sqlalchemy.sql.expression import column, table
# from  dag_s3_to_postgres_tbl import S3ToPostgresTransfer
import pandas as pd
from configparser import ConfigParser
import logging

# registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
default_args = {

    'owner':'airflow',
    'depends_on_past' : False,
    'start' : airflow.utils.dates.days_ago(1),
    'email': {'farha.akkalkot@themathcompany.com'},
    'email_on_failure':True,
    'email_on_retry':1,
    'retry_delay':timedelta(minutes=5)


}

'''Extracting data'''

def extract():
    config_file=ConfigParser()
    config_file.read('/opt/airflow/dags/config_file2.ini')
    aws_id=config_file['source_path']['aws_id']
    aws_secret=config_file['source_path']['aws_secret']
    client= boto3.client('s3',aws_access_key_id=aws_id,aws_secret_access_key=aws_secret)
    bucket_name=config_file['source_path']['bucket_name']
    file_name=config_file['source_path']['file_name']

    csv_obj = client.get_object(Bucket=bucket_name, Key=file_name)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_string))
    logging.info(df.info())
    return df.to_json(orient='columns')

'''cleaning'''

def cleaning(**kwargs):
    task_instance = kwargs['task_instance']
    df=task_instance.xcom_pull(task_ids='data_extract_from_S3')
    df=pd.read_json(df,orient='columns')
    logging.info(df.info())

    '''Converting column names from upper case to lower case'''
    df.columns=[x.lower() for x in df.columns]  
    logging.info(df.info())
    
    '''Dropping irrelevant columns'''
    df.drop(list(df.filter(regex='department')),axis=1,inplace=True)
    df.drop(['create_datetime'],axis=1,inplace=True)
    logging.info(df.info())

    '''Type casting'''
    df['dbsku']=pd.to_numeric(df.dbsku)
    df['dbsku']=df['dbsku'].fillna(0)
    df['dbsku']=df['dbsku'].astype('int64')
    df=df[df.dbsku!=0]
    logging.info(df.info())
    return df.to_json(orient='columns') 

'''Loading data to Azure SQL'''

def loading(**kwargs):
    task_instance = kwargs['task_instance']
    df=task_instance.xcom_pull(task_ids='cleaning_dataset')
    df=pd.read_json(df,orient='columns')
    driver = 'ODBC Driver 17 for SQL Server'
    config_file=ConfigParser()
    config_file.read('/opt/airflow/dags/config_file2.ini')
    username=config_file['sql_credentials']['username']
    password=config_file['sql_credentials']['password']
    server=config_file['sql_credentials']['server']
    db=config_file['sql_credentials']['db']
    conn_string=f'mssql://{username}:{password}@{server}/{db}?driver={driver}'

    engine=create_engine(conn_string,fast_executemany=True)

    conn=engine.connect()
    df.to_sql('clean_sku_data',con=conn,if_exists='append')

with DAG('dressbarn_ETL_Pipeline',default_args=default_args,schedule_interval='@once',start_date=datetime(2021,12,18)) as dag:

  
    data_extract_from_S3=PythonOperator(
        task_id = "data_extract_from_S3",
        python_callable=extract
    )

    cleaning_dataset=PythonOperator(
        task_id = "cleaning_dataset",
        python_callable=cleaning
    )

    loading_data_to_azure_sql=PythonOperator(
        task_id = "loading_data_to_azure_sql",
        python_callable=loading
    )

    data_extract_from_S3 >> cleaning_dataset >> loading_data_to_azure_sql