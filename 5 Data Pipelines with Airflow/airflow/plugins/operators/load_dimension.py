from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Inserts data in a target table from a given query. The user can specify a
    column to delete the data from the table before insert. It is recommended
    to pass a primary key as reference column in order to not duplicate rows in
    the dimension table.

    Args:
        table (str): Table name to perform data quality check.
        delete_from_column (str): Column name that goes in the WHERE clause.
            If None is given, the DELETE statement is skipped.
        sql_query (str): Query that performs the data quality check.
        redshift_conn_id (str): Redshift connection name from Airflow
            context keyword.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.

    Attributes:
        table (str): Table name to perform data quality check.
        delete_from_column (str): Column name that goes in the WHERE clause.
            If None is given, the DELETE statement is skipped.
        sql_query (str): Query that performs the data quality check.
        redshift_conn_id (str): Redshift connection name from Airflow
            context keyword.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.
    """

    ui_color = '#80BD9E'

    # auxilar query to delete the data from a given key before inserting
    delete_statement = """DELETE FROM {table}
                                 WHERE {column} in (
                                    SELECT DISTINCT {column}
                                    FROM ({query}))"""

    @apply_defaults
    def __init__(self,
                 table="",
                 delete_from_column=None,
                 sql_query="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.delete_from_column = delete_from_column
        self.sql_query = sql_query
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Creates a Redshift Hook, delete the rows that matches the given column
        and insert the new data of the SQL query into the table.

        Args:
            context (obj): context from run enviroment.
        """
        redshift = PostgresHook(self.redshift_conn_id)

        if(self.delete_from_column is not None):
            self.log.info(f"Deleting records from {self.table} by the column \
                    {self.delete_from_column}")
            redshift.run(LoadDimensionOperator.delete_statement.format(
                table=self.table,
                column=self.delete_from_column,
                query=self.sql_query
            ))

        self.log.info(f"Inserting into DIMENSION {self.table}")
        query = f"INSERT INTO {self.table}" + self.sql_query
        redshift.run(query)
