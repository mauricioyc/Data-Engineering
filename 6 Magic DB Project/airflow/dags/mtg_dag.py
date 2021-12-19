import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mtg_plugin import (HttpOperator, LoadDimensionOperator,
                                          LoadFactOperator, MtgJsonOperator,
                                          SqlQueries, StageToRedshiftOperator,
                                          UploadS3Operator)
from airflow.operators.subdag_operator import SubDagOperator

from subdag_check_data import check_data_quality_dag

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# ========= #
# VARIABLES #
# ========= #


def get_response(task_instance, task_id, key):
    '''Get xcom variable from other task by instance and id.'''
    return(task_instance.xcom_pull(task_ids=task_id)[key])


def get_bulk_data_link(response):
    '''get bulk-link from list of links returned by the Scryfall API.'''
    data = response.get('data')
    link = [link for link in data if link.get('type') == 'all_cards']
    return(link[0].get('download_uri'))


# Define user macros for templating
user_macros = {
    'get_response': get_response,
    'bulk_link': get_bulk_data_link
}

# AWS parameters
aws_credentials = 'aws_credentials'
s3_bucket = 'magic-db-project'
s3_key_template = '{main_folder}/{{{{ next_execution_date.year  }}}}/{{{{ next_execution_date.month  }}}}/{{{{ next_execution_date.day  }}}}/'  # noqa
filename_template = '{file_name}_{{{{ next_execution_date.year  }}}}_{{{{ next_execution_date.month  }}}}_{{{{ next_execution_date.day  }}}}.{extension}'  # noqa

# FILE parameters
gzip_flag = True
scryfall_s3_key = s3_key_template.format(main_folder='scryfall')
scryfall_json_format = f"JSON 's3://{s3_bucket}/scryfall_json_path.json'"
mtgjson_s3_key = s3_key_template.format(main_folder='mtgjson')

# ======= #
#   DAG   #
# ======= #
# DAG parameters
dag_identifier = "mtg_db"
start_date = datetime.now() - timedelta(days=2)

default_args = {
    "owner": "mauricio",
    "depends_on_past": False,
    "start_date": start_date,
    "email_on_failure": False,
    "email_on_retry": False,
    # "retry": 2,
    "retry_delay": timedelta(minutes=5),
}

# Create DAG
dag = DAG(dag_identifier,
          default_args=default_args,
          schedule_interval='0 8 * * *',
          catchup=False,
          user_defined_macros=user_macros)

# ========= #
#   DUMMY   #
# ========= #
start_operator = DummyOperator(task_id='DUMMY_begin_execution',  dag=dag)
end_of_staging = DummyOperator(task_id='DUMMY_end_of_staging',  dag=dag)
end_operator = DummyOperator(task_id='DUMMY_stop_execution',  dag=dag)

# ============ #
#   SCRYFALL   #
# ============ #
scryfall_get_link = HttpOperator(
    task_id="scryfall_get_link",
    dag=dag,
    endpoint="bulk-data",
    method="GET",
    data=None,
    headers=None,
    http_conn_id="scryfall_api",
    xcom_push_flag=True
)
scryfall_to_s3 = UploadS3Operator(
    task_id="scryfall_to_s3",
    dag=dag,
    download_link="{{ bulk_link(get_response(task_instance, 'scryfall_get_link', 'response')) }}",  # noqa
    aws_conn_id=aws_credentials,
    s3_bucket=s3_bucket,
    s3_key=scryfall_s3_key,
    filename=filename_template.format(file_name='all_cards', extension='json'),
    gzip_flag=gzip_flag,
    is_scryfall=True
)
stage_scryfall_to_redshift = StageToRedshiftOperator(
    task_id='stage_scryfall',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    schema='public',
    table='staging_scryfall',
    s3_bucket=s3_bucket,
    s3_key=scryfall_s3_key,
    aws_region="us-west-2",
    file_format=scryfall_json_format,
    time_format="auto"
)

# =================== #
#    MTGJSON PRICES   #
# =================== #
df_type = 'prices'
filename = filename_template.format(file_name=df_type, extension='csv')
get_mtgjson_prices = MtgJsonOperator(
    task_id=f"get_mtgjson_{df_type}",
    dag=dag,
    download_link="https://mtgjson.com/api/v5/AllPrices.json",
    aws_conn_id=aws_credentials,
    s3_bucket=s3_bucket,
    s3_key=mtgjson_s3_key,
    filename=filename,
    df_type=df_type,
    gzip_flag=gzip_flag
)
csv_to_s3_prices = UploadS3Operator(
    task_id=f"csv_to_s3_{df_type}",
    dag=dag,
    download_link=None,
    aws_conn_id=aws_credentials,
    s3_bucket=s3_bucket,
    s3_key=mtgjson_s3_key,
    filename=filename,
    gzip_flag=gzip_flag
)
stage_prices_to_redshift = StageToRedshiftOperator(
    task_id='stage_prices',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    schema='public',
    table='staging_prices',
    s3_bucket=s3_bucket,
    s3_key=os.path.join(mtgjson_s3_key, filename),
    aws_region="us-west-2",
    file_format='CSV',
    time_format="auto"
)

# =================== #
#    MTGJSON PRINTS   #
# =================== #
df_type = 'prints'
filename = filename_template.format(file_name=df_type, extension='csv')
get_mtgjson_prints = MtgJsonOperator(
    task_id=f"get_mtgjson_{df_type}",
    dag=dag,
    download_link="https://mtgjson.com/api/v5/AllPrintings.json",
    aws_conn_id=aws_credentials,
    s3_bucket=s3_bucket,
    s3_key=mtgjson_s3_key,
    filename=filename,
    df_type=df_type,
    gzip_flag=gzip_flag
)
csv_to_s3_prints = UploadS3Operator(
    task_id=f"csv_to_s3_{df_type}",
    dag=dag,
    download_link=None,
    aws_conn_id=aws_credentials,
    s3_bucket=s3_bucket,
    s3_key=mtgjson_s3_key,
    filename=filename,
    gzip_flag=gzip_flag
)
stage_prints_to_redshift = StageToRedshiftOperator(
    task_id='stage_prints',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    schema='public',
    table='staging_prints',
    s3_bucket=s3_bucket,
    s3_key=os.path.join(mtgjson_s3_key, filename),
    aws_region="us-west-2",
    file_format='CSV',
    time_format="auto"
)

# =============== #
#    DIMENSIONS   #
# =============== #
load_cards_table = LoadDimensionOperator(
    task_id='load_cards_dim_table',
    dag=dag,
    table="cards",
    sql_query=SqlQueries.cards_table_insert,
    redshift_conn_id="redshift"
)
load_sets_table = LoadDimensionOperator(
    task_id='load_sets_dim_table',
    dag=dag,
    table="sets",
    sql_query=SqlQueries.sets_table_insert,
    redshift_conn_id="redshift"
)
load_artists_table = LoadDimensionOperator(
    task_id='load_artists_dim_table',
    dag=dag,
    table="artists",
    sql_query=SqlQueries.artists_table_insert,
    redshift_conn_id="redshift"
)
load_time_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    table="time",
    sql_query=SqlQueries.time_table_insert,
    redshift_conn_id="redshift"
)
load_prices_table = LoadFactOperator(
    task_id='load_prices_fact_table',
    dag=dag,
    table="prices",
    sql_query=SqlQueries.prices_table_insert.format(
        date="{{ ds }}"
    ),
    redshift_conn_id="redshift"
)

# ==================== #
# QUALITY CHECK SUBDAG #
# ==================== #
task_id = "run_data_quality_checks"
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

# ============== #
#   CREATE DAG   #
# ============== #
start_operator >> scryfall_get_link >> scryfall_to_s3 >> \
    stage_scryfall_to_redshift >> end_of_staging

start_operator >> get_mtgjson_prices >> csv_to_s3_prices >> \
    stage_prices_to_redshift >> end_of_staging

start_operator >> get_mtgjson_prints >> csv_to_s3_prints >> \
    stage_prints_to_redshift >> end_of_staging

end_of_staging >> [load_cards_table,
                   load_sets_table,
                   load_artists_table,
                   load_time_table,
                   load_prices_table] >> run_quality_checks >> end_operator
