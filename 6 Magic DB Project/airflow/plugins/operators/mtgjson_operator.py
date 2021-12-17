import logging
import os
from collections import Mapping
from operator import add

import pandas as pd
import requests
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class MtgJsonOperator(BaseOperator):

    template_fields = ('download_link', 's3_bucket', 's3_key', 'filename',)
    template_ext = ()

    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 download_link=None,
                 aws_conn_id='',
                 s3_bucket='',
                 s3_key='',
                 filename='',
                 df_type='prices',
                 gzip_flag=False,
                 * args, **kwargs):

        super(MtgJsonOperator, self).__init__(*args, **kwargs)
        self.download_link = download_link
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.gzip_flag = gzip_flag
        self.df_type = df_type
        if(self.gzip_flag):
            self.filename = filename + '.gz'
        else:
            self.filename = filename

    def download_file(self, download_link):
        try:
            logging.info(f"Downloading from link: {download_link}")
            response = requests.get(download_link)
            return(response.json())

        except Exception as e:
            logging.error(e)
            raise

    def save_file(self, df, filename):
        if(self.gzip_flag):
            compression = 'gzip'
            logging.info("Compressing and saving temporary file...")
        else:
            compression = None
            logging.info("Saving json to temporary file...")

        df.to_csv(filename, compression=compression)

    @staticmethod
    def create_pandas_df(file_type, data):
        if(file_type == 'prices'):
            currency = {
                'tcgplayer':  'USD',
                'cardmarket':  'EUR',
                'cardkingdom':  'USD',
                'cardhoarder':  'USD'
            }
            columns = ["card_id", "online_paper", "store", "price_type",
                       "card_type", "dt", "price"]

            df = pd.DataFrame(
                MtgJsonOperator.flattenDict(data), columns=columns)
            df = df[~df.card_type.isin(['USD', 'EUR'])]
            df["currency"] = df.store.map(currency)
        else:
            columns = ['card_id', 'name', 'edition']

            df = pd.DataFrame(columns=columns)
            for edition in data:
                cards = data.get(edition).get('cards')
                if(len(cards) > 0):
                    aux = pd.DataFrame(cards)
                    aux = aux[['uuid', 'name']]
                    aux['edition'] = edition
                    aux.columns = columns
                    df = pd.concat([df, aux], axis=0)
        df = df.drop_duplicates()
        return(df)

    @staticmethod
    def flattenDict(d, join=add, lift=lambda x: (x,)):
        results = []
        _FLAG_FIRST = object()

        def visit(subdict, results, partialKey):
            for k, v in subdict.items():
                newKey = lift(k) if partialKey == _FLAG_FIRST else join(
                    partialKey, lift(k))
                if isinstance(v, Mapping):
                    visit(v, results, newKey)
                else:
                    results.append(add(newKey, lift(v)))
        visit(d, results, _FLAG_FIRST)
        return results

    def check_if_file_exists(self, filename):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        logging.info(
            f'Checking files in bucket: {self.s3_bucket} with prefix: {self.s3_key}...')  # noqa

        key_list = s3_hook.list_keys(
            bucket_name=self.s3_bucket, prefix=self.s3_key)

        if(key_list):
            return(os.path.join(self.s3_key, filename) in key_list)
        else:
            return(False)

    def execute(self, context):

        if(self.check_if_file_exists(self.filename)):
            logging.info(
                f"File {os.path.join(self.s3_key, self.filename)} already exists. Skipping download...")  # noqa
        else:
            logging.info(f"Downloading from link: {self.download_link}")
            json_data = self.download_file(self.download_link)['data']

            logging.info(f"Getting dataframe for {self.df_type}")
            df = self.create_pandas_df(self.df_type, json_data)
            self.save_file(df, self.filename)
