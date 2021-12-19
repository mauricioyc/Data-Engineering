from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Performs Data Quality Check for a given query and table. The query should
    return a positive or True value for the test it needs to perform. In the
    case that the returned value of the query is False or <= 0, an error in
    raised.

    Args:
        redshift_conn_id (str): Redshift connection name from Airflow
            context keyword.
        table (str): Table name to perform data quality check.
        sql_query (str): Query that performs the data quality check.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.

    Attributes:
        redshift_conn_id (str): Redshift connection name from Airflow
            context keyword.
        table (str): Table name to perform data quality check.
        sql_query (str): Query that performs the data quality check.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.

    """
    ui_color = '#FFCE30'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 * args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def check_data_positive(self, redshift):
        """
        Check if the data for a given querty is greater than 0 or True. If it
        does not pass the teste, an error is raised.

        Args:
            redshift_conn_id (:obj:`str`): redshift connection name from Airflow
                context keyword.
        """

        records = redshift.get_records(self.sql_query)

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(
                f"Data quality check failed. {self.table} returned no results")

        num_records = records[0][0]
        if num_records <= 0:
            raise ValueError(
                f"Data quality check failed for {self.table}. It was \
                      expected a value greater than 0 or True")
        self.log.info(
            f"""Data quality on table {self.table} check passed with \
                {num_records}""")

    def execute(self, context):
        """
        Creates a Redshift Hook and performs the quality check.

        Args:
            context (obj): context from run enviroment.
        """
        redshift = PostgresHook(self.redshift_conn_id)
        self.check_data_positive(redshift)
