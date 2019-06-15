import datetime
import logging
import os
import sys

sys.path.insert(0,"~/airflow/dags/S3-to-Redshift/")

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable

import sql

dag_vars = Variable.get("S3-to-Redshift-Vars", deserialize_json=True)

def load_songs_to_redshift(*args,**kwargs):
    """
    Loads songs data from S3 to redshift using parameters supplied by the 
    calling airflow task
    """

    aws_hook_name = kwargs["params"]["aws_hook"]
    redshift_hook_name = kwargs["params"]["redshift_hook"]
    songs_data_location = kwargs["params"]["songs_data_location"]

    aws_hook = AwsHook(aws_hook_name)
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook(redshift_hook_name)
    
    sql_statement = sql.stage_songs.format(songs_data_location,
            credentials.access_key, credentials.secret_key)
    redshift_hook.run(sql_statement)


with DAG(
        dag_id="s3_to_redshift",
        schedule_interval=None, 
        start_date=datetime.datetime.now()-datetime.timedelta(hours = 4),
        max_active_runs=1,
        ) as dag:
    
    init_tables_task = PostgresOperator( 
        task_id = "init_tables",
        postgres_conn_id = "million-songs-warehouse",
        sql = sql.init_tables)

    load_songs_task = PythonOperator(
        task_id = "stage_songs",
        python_callable = load_songs_to_redshift,
        params = {
            "aws_hook": "uda-aws-s3",
            "redshift_hook": "million-songs-warehouse",
            "songs_data_location": dag_vars["songs_data"]
        },
        provide_context= True
    )

    extract_artists_task = PostgresOperator(
        task_id = "load_artists",
        postgres_conn_id="million-songs-warehouse",
        sql = sql.load_artists
    )

    extract_songs_task = PostgresOperator(
        task_id = "load_songs",
        postgres_conn_id="million-songs-warehouse",
        sql = sql.load_songs
    )

    init_tables_task>>load_songs_task>>[extract_artists_task, extract_songs_task]
    