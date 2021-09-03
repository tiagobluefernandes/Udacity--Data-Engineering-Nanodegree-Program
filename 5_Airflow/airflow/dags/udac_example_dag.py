from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from helpers import SqlQueries, DataQualityQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval="@once"
          schedule_interval='0 * * * *'
        )

#start_operator = DummyOperator(
#    task_id='Begin_execution',  
#    dag=dag)

start_operator = PostgresOperator(
    task_id = 'Begin_execution_and_Create_tables',
    dag = dag,
    postgres_conn_id = 'redshift',
    sql = 'create_tables.sql'
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = "public.staging_events",
    region = "us-west-2",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    json_path = 's3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'public.staging_songs',
    region = "us-west-2",
    s3_bucket = "udacity-dend",
    s3_key = "song_data/A/A/A",
    json_path = 'auto'
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'public.songplays',
    query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'public.users',
    #query = f"(userid,firstname,lastname,gender,level) {SqlQueries.user_table_insert}",
    query = SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'public.songs',
    query = SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'public.artists',
    query = SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'public."time"',
    query = SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    redshift_conn_id = "redshift",
    #tables = ['public.songplays', 'public.users', 'public.songs', 'public.artists', 'public.time'],
    sql_stmt = [
        DataQualityQueries.table_staging_songs_not_empty,
        DataQualityQueries.table_staging_events_not_empty,
        DataQualityQueries.table_staging_songs_not_empty_col_song_id,
        DataQualityQueries.table_staging_songs_not_empty_col_artist_id,
    ],
    expected_result = [
        0,
        0,
        0,
        0,
    ]
)

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
