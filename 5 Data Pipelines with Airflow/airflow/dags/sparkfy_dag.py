from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (LoadDimensionOperator, LoadFactOperator,
                               StageToRedshiftOperator)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from helpers import SqlQueries

from subdag_check_data import check_data_quality_dag

# start date for the main and sub DAG
start_date = datetime.now() - timedelta(days=1)

# DAG id for main and sub DAG
dag_identifier = 'sparkify_dag'

# default arguments for the main DAG
default_args = {
    'owner': 'mauricio',
    'start_date': start_date,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'catchup_by_default': False,
    'email_on_retry': False
}

# main DAG
dag = DAG(dag_identifier,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="0 * * * *",
          max_active_runs=1,
          catchup=False
          )

# dummy start task
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# stage log events table task. \
# here it is possible to pass a context execution date variable to the \
# s3_key for example: \
# s3_key="log_data/{execution_date.year}/{execution_date.month}/
# {{ execution_date.strftime('%Y-%m-%d') }}-events.json
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    schema='public',
    table='staging_events',
    s3_bucket="udacity-dend",
    s3_key="log_data",
    aws_region="us-west-2",
    json_format="s3://udacity-dend/log_json_path.json",
    time_format="epochmillisecs"
)

# stage song data
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    schema='public',
    table='staging_songs',
    s3_bucket="udacity-dend",
    s3_key="song_data",
    aws_region="us-west-2",
    json_format="auto",
    time_format="auto"
)

# load data to the songplays table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift"
)

# load data to the users table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    delete_from_column='userid',
    sql_query=SqlQueries.user_table_insert,
    redshift_conn_id="redshift"
)

# load data to the songs table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    delete_from_column='song_id',
    sql_query=SqlQueries.song_table_insert,
    redshift_conn_id="redshift"
)

# load data to the artists table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    delete_from_column='artist_id',
    sql_query=SqlQueries.artist_table_insert,
    redshift_conn_id="redshift"
)

# load data to the time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    delete_from_column='start_time',
    sql_query=SqlQueries.time_table_insert,
    redshift_conn_id="redshift"
)

# quality check SubDag
task_id = "Run_data_quality_checks"
run_quality_checks = SubDagOperator(
    subdag=check_data_quality_dag(
        parent_dag_name=dag_identifier,
        task_id=task_id,
        redshift_conn_id="redshift",
        start_date=start_date
    ),
    task_id=task_id,
    dag=dag
)

# dummy end DAG
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# create DAG graph
start_operator >> [stage_events_to_redshift,
                   stage_songs_to_redshift] >> load_songplays_table >> \
    [load_user_dimension_table, load_song_dimension_table,
     load_artist_dimension_table, load_time_dimension_table] >> \
    run_quality_checks >> end_operator
