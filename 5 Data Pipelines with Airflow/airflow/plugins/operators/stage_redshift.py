import os

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Load data from a given path to the Redshift. This class is optmized to 
    allow the user to copy data from a S3 bucket to the Redshift.

    Args:
        schema (str): Schema name of the destination of the copy statement.
        table (str): Table name of the destination of the copy statement.
        redshift_conn_id (str): Redshift connection name from Airflow
                    context keyword.
        s3_bucket (str): Bucket name of the data source.
        s3_key (str): Key of files in the source bucket. It is possible to pass
            context variables to perform partitioning load.
        aws_credentials_id (str): AWS credentials from Airflow context keyword.
        aws_region (str): AWS region of the data source.
        json_format (str): JSON manifest location to format data.
        time_format (str): Timestampo format to format data.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.
    Attributes:
        schema (str): Schema name of the destination of the copy statement.
        table (str): Table name of the destination of the copy statement.
        redshift_conn_id (str): Redshift connection name from Airflow
                    context keyword.
        s3_bucket (str): Bucket name of the data source.
        s3_key (str): Key of files in the source bucket. It is possible to pass
            context variables to perform partitioning load.
        aws_credentials_id (str): AWS credentials from Airflow context keyword.
        aws_region (str): AWS region of the data source.
        json_format (str): JSON manifest location to format data.
        time_format (str): Timestampo format to format data.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.
    """

    template_fields = ("s3_key",)

    ui_color = '#358140'

    copy_sql = """
        COPY {schema}.{table}
        FROM '{json_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        REGION '{aws_region}'
        FORMAT AS JSON '{json_format}'
        TIMEFORMAT AS '{time_format}'
        EMPTYASNULL
        TRIMBLANKS
        BLANKSASNULL
        COMPUPDATE OFF
        STATUPDATE OFF;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 schema="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 aws_region="",
                 json_format="",
                 time_format="auto",
                 * args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.aws_region = aws_region
        self.json_format = json_format
        self.time_format = time_format

    def execute(self, context):
        """
        Creates a Redshift Hook, get the AWS credentials, checks if the 
        destination table exists, delete previous temp staging tables and
        copy the data to the Redshift.

        Args:
            context (obj): context from run enviroment.
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Checking if table exists")
        records = redshift.get_records(
            f"""SELECT 1 FROM pg_tables
                WHERE schemaname='{self.schema}'
                      AND tablename='{self.table}'""")

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(
                f"Could not find {self.schema}.{self.table} in Redshift")
        else:
            self.log.info(
                "Clearing data from stage destination Redshift table")
            redshift.run(f"TRUNCATE {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        json_path = os.path.join(
            f"s3://{self.s3_bucket}", f"{rendered_key}")

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            schema=self.schema,
            table=self.table,
            json_path=json_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            aws_region=self.aws_region,
            json_format=self.json_format,
            time_format=self.time_format
        )
        redshift.run(formatted_sql)
