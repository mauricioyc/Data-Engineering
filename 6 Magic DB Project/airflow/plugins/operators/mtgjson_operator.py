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
    """
    From a giving download link, checks if the file to be downloaded exists in
    S3. If not exists, the file is transformed and downloaded locally. If it
    exists, nothing is done.

    Args:
        download_link (str): Link to download the data.
        aws_conn_id (str): AWS connection name from Airflow
            context keyword.
        s3_bucket (str): Bucket name of the data source.
        s3_key (str): Key of files in the source bucket. It is possible to pass
            context variables to perform partitioning load.
        redshift_conn_id (str): Redshift connection name from Airflow
            context keyword.
        filename (str): filename to be saved locally. If gzip is used, '.gz'
            is concatenated in the end of the file. This filename is also
            used to check if it exists in S3.
        df_type (str): type of the data to be saved locally. If prices, the
            respective transformation is applied, else the transformation for
            printings is applied.
        gzip_flag (boolean): if True, the file is gzipped before saving
            locally.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.

    Attributes:
        download_link (str): Link to download the data.
        aws_conn_id (str): AWS connection name from Airflow
            context keyword.
        s3_bucket (str): Bucket name of the data source.
        s3_key (str): Key of files in the source bucket. It is possible to pass
            context variables to perform partitioning load.
        redshift_conn_id (str): Redshift connection name from Airflow
            context keyword.
        filename (str): filename to be saved locally. If gzip is used, '.gz'
            is concatenated in the end of the file. This filename is also
            used to check if it exists in S3.
        df_type (str): type of the data to be saved locally. If prices, the
            respective transformation is applied, else the transformation for
            printings is applied.
        gzip_flag (boolean): if True, the file is gzipped before saving
            locally.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.
    """
    template_fields = ('download_link', 's3_bucket', 's3_key', 'filename',)
    template_ext = ()

    ui_color = '#288BA8'

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
        """
        Get response from download link.

        Args:
            download_link (str): Link to download the data.
        """
        try:
            logging.info(f"Downloading from link: {download_link}")
            response = requests.get(download_link)
            return(response.json())

        except Exception as e:
            logging.error(e)
            raise

    def save_file(self, df, filename):
        """
        Save df locally. If gzip_flag is True, the file is compressed before
        saving.

        Args:
            df (DataFrame): Pandas DataFrame to be saved as CSV locally.
                filename (str): name of the CSV.
        """
        if(self.gzip_flag):
            compression = 'gzip'
            logging.info("Compressing and saving temporary file...")
        else:
            compression = None
            logging.info("Saving json to temporary file...")

        df.to_csv(filename, compression=compression, index=False)

    @staticmethod
    def create_pandas_df(file_type, data):
        """
        Transforms the response data in a dict format to a Pandas DataFrame.
        If the data is for the 'prices' table, the transformation unnests the
        data in the correspoding columns. Otherwise, if takes the 'prints' data
        and filters only the necessary keys to be created in the DataFrame.

        Args:
            file_type (str): Can be 'prices' or 'prints'. If prices, transforms
                the DataFrame respectively.
            data (dict): data to be parsed into a Pandas DataFrame.
        """
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
            columns = ['card_id', 'name', 'collector_number', 'edition']

            df = pd.DataFrame(columns=columns)
            for edition in data:
                cards = data.get(edition).get('cards')
                if(len(cards) > 0):
                    aux = pd.DataFrame(cards)
                    aux = aux[['uuid', 'name', 'number']]
                    aux['edition'] = edition
                    aux.columns = columns
                    df = pd.concat([df, aux], axis=0)
        df = df.drop_duplicates()
        return(df)

    @staticmethod
    def flattenDict(d, join=add, lift=lambda x: (x,)):
        """
        Unnest a dicitionary into records.

        Args:
            d (dict): Dictionary to be unnested.
            join (dict): method to join the unnested data.
            lift (obj): function to concatenate the unnested data.
        """
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
        """
        Checks if file exists in S3.

        Args:
            filename (str): Filename to be checked if exists in S3.
        """
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
        """
        Check if file exists in S3, get file response from download link,
        transforms it into a Pandas DataFrame and saves locally.

        Args:
            context (obj): context from run enviroment.
        """
        if(self.check_if_file_exists(self.filename)):
            logging.info(
                f"File {os.path.join(self.s3_key, self.filename)} already exists. Skipping download...")  # noqa
        else:
            logging.info(f"Downloading from link: {self.download_link}")
            json_data = self.download_file(self.download_link)['data']

            logging.info(f"Getting dataframe for {self.df_type}")
            df = self.create_pandas_df(self.df_type, json_data)
            self.save_file(df, self.filename)
