import gzip
import json
import logging
import os

import requests
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class UploadS3Operator(BaseOperator):
    """
    Uploads a local file to S3 and delete it. If the files already exists in S3
    nothing is done. In the case that the file to be uploaded is from Scryfall.
    A transformation in the JSON is done to be record wise.

    Args:
        download_link (str): Link to download the data.
        aws_conn_id (str): AWS connection name from Airflow
            context keyword.
        s3_bucket (str): Bucket name of the data source.
        s3_key (str): Key of files in the source bucket. It is possible to pass
            context variables to perform partitioning load.
        redshift_conn_id (str): Redshift connection name from Airflow
            context keyword.
        filename (str): filename to be saved in S3. If gzip is used, '.gz'
            is concatenated in the end of the file. This filename is also
            used to check if it exists in S3.
        gzip_flag (boolean): if True, the file is gzipped before saving
            locally.
        is_scryfall (boolean): Checks if the file to be uploaded is from
            Skyfall.
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
        filename (str): filename to be saved in S3. If gzip is used, '.gz'
            is concatenated in the end of the file. This filename is also
            used to check if it exists in S3.
        gzip_flag (boolean): if True, the file is gzipped before saving
            locally.
        is_scryfall (boolean): Checks if the file to be uploaded is from
            Skyfall.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.
    """
    template_fields = ('download_link', 's3_bucket', 's3_key', 'filename',)
    template_ext = ()

    ui_color = '#f4A460'

    @apply_defaults
    def __init__(self,
                 download_link=None,
                 aws_conn_id='',
                 s3_bucket='',
                 s3_key='',
                 filename='',
                 gzip_flag=False,
                 is_scryfall=False,
                 * args, **kwargs):

        super(UploadS3Operator, self).__init__(*args, **kwargs)
        self.download_link = download_link
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.gzip_flag = gzip_flag
        self.is_scryfall = is_scryfall
        if(gzip_flag):
            self.filename = filename + '.gz'

        else:
            self.filename = filename

    def check_if_file_exists(self, s3_hook, filename):
        """
        Check if file exists in S3.

        Args:
            s3_hook (obj): S3 Hook object.
            filename (str): filename to be checked in S3.
        """
        logging.info(
            f'Checking files in bucket: {self.s3_bucket} with prefix: {self.s3_key}...')  # noqa

        key_list = s3_hook.list_keys(
            bucket_name=self.s3_bucket, prefix=self.s3_key)

        if(key_list):
            return(os.path.join(self.s3_key, self.filename) in key_list)
        else:
            return(False)

    def download_file(self, s3_hook):
        """
        Download a file if a download link is passed. If the file passed is
        form Scryfall, a transformation is applied.

        Args:
            s3_hook (obj): S3 Hook object.
        """
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
                if(self.is_scryfall):
                    logging.info("Editing file from Scryfall...")
                    response = [self.select_columns(
                        data) for data in response.json()]
                    for line in response:
                        with gzip.open(self.filename, "at", encoding='utf-8') as file:  # noqa
                            string = json.dumps(line, ensure_ascii=False)
                            file.write(string + "\n")
                else:
                    with gzip.open(self.filename, 'wt') as file:
                        json.dump(response.json(), file)

            else:
                if(self.is_scryfall):
                    for line in response:
                        with open(self.filename, "a", encoding='utf-8') as file:  # noqa
                            string = json.dumps(line, ensure_ascii=False)
                            file.write(string + "\n")
                else:
                    logging.info("Saving json to temporary file...")
                    with open(self.filename, 'w', encoding='utf-8') as file:
                        json.dump(response.json(), file)
            return(True)

    @staticmethod
    def select_columns(data):
        """
        Columns of Scryfall data to be filtered.

        Args:
            data (dict): Dictionary with the data to be filtered.
        """
        field_list = ['id', 'name', 'lang', 'released_at', 'layout',
                      'mana_cost', 'cmc', 'type_line', 'oracle_text', 'power',
                      'toughness', 'colors', 'color_identity', 'keywords',
                      'legalities', 'reserved', 'foil', 'nonfoil', 'oversized',
                      'promo', 'reprint', 'variation', 'set_id', 'set',
                      'set_name', 'set_type', 'collector_number', 'digital',
                      'rarity', 'artist', 'artist_ids', 'border_color',
                      'frame', 'full_art', 'textless', 'booster',
                      'story_spotlight', 'printed_name', 'printed_type_line',
                      'printed_text', 'security_stamp', 'loyalty', 'watermark',
                      'produced_mana', 'color_indicator',
                      'tcgplayer_etched_id', 'content_warning',
                      'life_modifier', 'hand_modifier']
        return({key: data.get(key) for key in field_list})

    def execute(self, context):
        """
        Creates a S3 Hook, if a download link is passed, the file is
        downloaded and uploaded to S3. If a download link is not passed, it
        tries to upload a file locally with the given filename.

        Args:
            context (obj): context from run enviroment.
        """
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
