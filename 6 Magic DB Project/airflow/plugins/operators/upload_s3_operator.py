import gzip
import json
import logging
import os

import requests
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class UploadS3Operator(BaseOperator):

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
                 gzip_flag=False,
                 * args, **kwargs):

        super(UploadS3Operator, self).__init__(*args, **kwargs)
        self.download_link = download_link
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.gzip_flag = gzip_flag
        if(gzip_flag):
            self.filename = filename + '.gz'

        else:
            self.filename = filename

    def check_if_file_exists(self, s3_hook, filename):
        logging.info(
            f'Checking files in bucket: {self.s3_bucket} with prefix: {self.s3_key}...')  # noqa

        key_list = s3_hook.list_keys(
            bucket_name=self.s3_bucket, prefix=self.s3_key)

        if(key_list):
            return(os.path.join(self.s3_key, self.filename) in key_list)
        else:
            return(False)

    def download_file(self, s3_hook):

        if(self.check_if_file_exists(s3_hook, self.filename)):
            logging.info(
                f"File {os.path.join(self.s3_key, self.filename)} already exists. Skipping download...")  # noqa
            return(False)
        else:
            logging.info('File not found in S3...')
            try:
                logging.info(f"Downloading from link: {self.download_link}")
                response = requests.get(self.download_link)

            except Exception as e:
                logging.error(e)
                raise

            if(self.gzip_flag):
                logging.info("Compressing and saving temporary file...")
                with gzip.open(self.filename, 'wt') as file:
                    json.dump(response.json(), file)

            else:
                logging.info("Saving json to temporary file...")
                with open(self.filename, 'w', encoding='utf-8') as file:
                    json.dump(response.json(), file)
            return(True)

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        if(self.download_link):
            load_file = self.download_file(s3_hook)
        else:
            load_file = os.path.isfile(self.filename)

        logging.info(f"Load File to S3 is {load_file}")
        if(load_file):
            s3_file_path = os.path.join(self.s3_key, self.filename)
            logging.info(f"Loading temporary file {self.filename} to S3...")
            logging.info(f"S3 path is {s3_file_path}")
            s3_hook.load_file(
                filename=self.filename,
                key=s3_file_path,
                bucket_name=self.s3_bucket,
                replace=True
            )

        logging.info(f"Deleting temporary file {self.filename} ...")
        if os.path.isfile(self.filename):
            os.remove(self.filename)
        else:
            logging.info(f"File does not exist {self.filename} skipping...")
