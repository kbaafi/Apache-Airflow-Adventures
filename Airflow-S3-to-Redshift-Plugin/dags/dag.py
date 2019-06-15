import datetime
import sys

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import S3ToRedshiftOperator
from airflow.operators import PostgresHasRowsOperator
from airflow.models import Variable

from airflow import DAG

sys.path.insert(0,"~/airflow/dags/s3-redshift-helpers/")

import dag_sql

dag_vars = Variable.get("songs_s3_buckets", deserialize_json=True)
input_bucket = dag_vars["bucket"]
log_data = dag_vars["logs"]
song_data = dag_vars["songs"]

stage_songs_params = []
stage_songs_params.append(" json 'auto' ")

with DAG(dag_id="s3_to_redshift-w-plugin",schedule_interval=None, 
        start_date=datetime.datetime.now()-datetime.timedelta(hours = 4)
        ) as dag:

    init_tables = PostgresOperator( dag = dag,
        task_id = "init_tables",
        postgres_conn_id = "redshift_songs",
        sql = dag_sql.init_tables
    )
    
    verify_staged_songs = PostgresHasRowsOperator(
        task_id = "verify_imported_records",
        table = "staging_songs", 
        connection_id = "redshift_songs")
    
    stage_songs_from_s3 = S3ToRedshiftOperator(
        task_id             = "stage-songs-to-redshift",
        redshift_conn_id    = "redshift_songs",
        aws_conn_id         = "s3_songs",
        table               = "staging_songs",
        s3_bucket           = input_bucket,
        s3_key              = song_data,
        overwrite           = True,
        copy_params         = stage_songs_params
    )

    extract_artists = PostgresOperator(
        task_id = "load_artists",
        postgres_conn_id="redshift_songs",
        sql = dag_sql.load_artists
    )

    extract_songs = PostgresOperator(
        task_id = "load_songs",
        postgres_conn_id="redshift_songs",
        sql = dag_sql.load_songs
    ) 
    init_tables>>stage_songs_from_s3
    stage_songs_from_s3>>verify_staged_songs
    verify_staged_songs>>[extract_artists,extract_songs]

