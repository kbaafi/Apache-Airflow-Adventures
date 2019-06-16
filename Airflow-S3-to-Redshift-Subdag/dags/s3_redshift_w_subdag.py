"""
@author : Kofi Baafi


"""

import datetime
import logging
import sys

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable

from create_copy_verify_subdag import get_s3_to_redshift_dag
from dag_sql import *

dag_vars            = Variable.get("songs_s3_buckets", deserialize_json=True)
input_bucket        = dag_vars["bucket"]
log_data            = dag_vars["logs"]
song_data           = dag_vars["songs"]

redshift_conn_id    = "redshift_songs"
aws_conn_id         = "s3_songs"
stage_songs_table   = "staging_songs"

stage_songs_params  = []
stage_songs_params.append(" json 'auto' ")

dag_id              = "songs_etl"
songs_copy_task_id  = "copy_songs_subdag"

start_date          = datetime.datetime.now()-datetime.timedelta(hours = 4)

with DAG(
        dag_id=dag_id,
        schedule_interval=None, 
        start_date=start_date,
        max_active_runs=1,
    ) as dag:

    create_tables = PostgresOperator(
        task_id = "init_tables",
        postgres_conn_id = redshift_conn_id,
        sql  = init_tables
    )

    
    stage_songs_to_redshift = SubDagOperator(
        dag = dag,
        task_id = songs_copy_task_id,
        subdag = get_s3_to_redshift_dag(
            dag_id,
            songs_copy_task_id,
            redshift_conn_id,
            aws_conn_id,
            stage_songs_table,
            create_staging_songs_table,
            input_bucket,
            song_data,
            False,
            stage_songs_params,
            start_date = start_date
        )
    )

    extract_artists = PostgresOperator(
        task_id = "load_artists",
        postgres_conn_id=redshift_conn_id,
        sql = load_artists
    )

    extract_songs = PostgresOperator(
        task_id = "load_songs",
        postgres_conn_id=redshift_conn_id,
        sql = load_songs
    ) 
    create_tables >> stage_songs_to_redshift >> [extract_artists,extract_songs]





