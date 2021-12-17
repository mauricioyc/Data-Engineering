import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mtg_plugin import (HttpOperator, MtgJsonOperator,
                                          UploadS3Operator)

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def get_response(task_instance, task_id, key):
    return(task_instance.xcom_pull(task_ids=task_id)[key])


def get_bulk_data_link(response):
    data = response.get('data')
    link = [link for link in data if link.get('type') == 'all_cards']
    return(link[0].get('download_uri'))


user_macros = {
    'get_response': get_response,
    'bulk_link': get_bulk_data_link
}

default_args = {
    "owner": "mauricio",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=2),
    "email_on_failure": False,
    "email_on_retry": False,
    # "retry": 2,
    "retry_delay": timedelta(minutes=5),
}

gzip_flag = True
aws_credentials = 'aws_credentials'
s3_bucket = 'magic-db-project'
s3_key_template = '{main_folder}/{{{{ next_execution_date.year  }}}}/{{{{ next_execution_date.month  }}}}/{{{{ next_execution_date.day  }}}}/'  # noqa
filename_template = '{file_name}_{{{{ next_execution_date.year  }}}}_{{{{ next_execution_date.month  }}}}_{{{{ next_execution_date.day  }}}}.{extension}'  # noqa
scryfall_s3_key = s3_key_template.format(main_folder='scryfall')
mtgjson_s3_key = s3_key_template.format(main_folder='mtgjson')

dag = DAG("mtg_gather_data",
          default_args=default_args,
          schedule_interval='0 8 * * *',
          catchup=False,
          user_defined_macros=user_macros)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

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
    gzip_flag=gzip_flag
)

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

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> scryfall_get_link >> scryfall_to_s3 >> end_operator
start_operator >> get_mtgjson_prices >> csv_to_s3_prices >> end_operator
start_operator >> get_mtgjson_prints >> csv_to_s3_prints >> end_operator
