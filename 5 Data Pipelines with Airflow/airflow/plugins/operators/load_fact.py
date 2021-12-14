from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Inserts data in a target table from a given query.

    Args:
        table (str): Table name to perform data quality check.
        sql_query (str): Query that performs the data quality check.
        redshift_conn_id (str): Redshift connection name from Airflow
            context keyword.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.

    Attributes:
        table (str): Table name to perform data quality check.
        sql_query (str): Query that performs the data quality check.
        redshift_conn_id (str): Redshift connection name from Airflow
            context keyword.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 sql_query="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_query = sql_query
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Creates a Redshift Hook and insert the new data of the SQL query
        into the table.

        Args:
            context (obj): context from run enviroment.
        """
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info(f"Inserting into FACT {self.table}")
        query = f"INSERT INTO {self.table}" + self.sql_query
        redshift.run(query)
