from airflow import DAG
from airflow.operators.mtg_plugin import DataQualityOperator


def check_data_quality_dag(parent_dag_name,
                           task_id,
                           redshift_conn_id,
                           *args, **kwargs):
    """
    Airflow SubDAG that creates data quality checks to ensure that the main DAG
    has run correctly. It checks the total rows of the final tables, if the
    primary or foreign keys are not null and if there are duplicated keys in
    the destination table.

    Args:
        parent_dag_name (str): Parent DAG name.
        task_id (str): Current SubDAG name.
        redshift_conn_id (str): Redshift connection name from Airflow
            context keyword.
        *args: Arbitrary argument list.
        **kwargs: Arbitrary keyword arguments.

    Returns:
        DAG: Airflow DAG to be incorporated in the main DAG.

    """

    # support query to count all rows of a table
    check_rows_query = """SELECT COUNT(*) FROM {table}"""

    # support query to count nulls of a given column from a table
    check_null_query = """SELECT SUM(CASE WHEN {column} IS NULL 
                                          THEN 1 ELSE 0 END) < 1
                          FROM {table}
    """

    # support query to count duplicates of a given column from a table
    check_duplicated_query = """SELECT COUNT({column}) <= 0
                                FROM (SELECT {column},
                                             count({column}) as duplicated
                                    FROM {table}
                                    GROUP BY {column}
                                    HAVING duplicated > 1
                                )
    """

    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    # ======= #
    #  CARDS  #
    # ======= #
    table = "cards"
    primary_key = "card_id"
    # task to count rows of songplays
    check_rows_cards = DataQualityOperator(
        task_id=f"check_rows_{table}",
        table=table,
        dag=dag,
        sql_query=check_rows_query.format(table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to count nulls of userid
    check_pk_cards = DataQualityOperator(
        task_id=f"check_pk_{table}",
        table=table,
        dag=dag,
        sql_query=check_null_query.format(column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to check duplicated primary key on userid
    check_duplicated_cards = DataQualityOperator(
        task_id=f"check_duplicated_{table}",
        table=table,
        dag=dag,
        sql_query=check_duplicated_query.format(
            column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )

    # ====== #
    #  SETS  #
    # ====== #
    table = "sets"
    primary_key = "set_id"
    # task to count rows of songplays
    check_rows_sets = DataQualityOperator(
        task_id=f"check_rows_{table}",
        table=table,
        dag=dag,
        sql_query=check_rows_query.format(table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to count nulls of userid
    check_pk_sets = DataQualityOperator(
        task_id=f"check_pk_{table}",
        table=table,
        dag=dag,
        sql_query=check_null_query.format(column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to check duplicated primary key on userid
    check_duplicated_sets = DataQualityOperator(
        task_id=f"check_duplicated_{table}",
        table=table,
        dag=dag,
        sql_query=check_duplicated_query.format(
            column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )

    # ======= #
    # ARTISTS #
    # ======= #
    table = "artists"
    primary_key = "artist_id"
    # task to count rows of songplays
    check_rows_artists = DataQualityOperator(
        task_id=f"check_rows_{table}",
        table=table,
        dag=dag,
        sql_query=check_rows_query.format(table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to count nulls of userid
    check_pk_artists = DataQualityOperator(
        task_id=f"check_pk_{table}",
        table=table,
        dag=dag,
        sql_query=check_null_query.format(column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to check duplicated primary key on userid
    check_duplicated_artists = DataQualityOperator(
        task_id=f"check_duplicated_{table}",
        table=table,
        dag=dag,
        sql_query=check_duplicated_query.format(
            column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )

    # ====== #
    #  TIME  #
    # ====== #
    table = "time"
    primary_key = "dt"
    # task to count rows of songplays
    check_rows_time = DataQualityOperator(
        task_id=f"check_rows_{table}",
        table=table,
        dag=dag,
        sql_query=check_rows_query.format(table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to count nulls of userid
    check_pk_time = DataQualityOperator(
        task_id=f"check_pk_{table}",
        table=table,
        dag=dag,
        sql_query=check_null_query.format(column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to check duplicated primary key on userid
    check_duplicated_time = DataQualityOperator(
        task_id=f"check_duplicated_{table}",
        table=table,
        dag=dag,
        sql_query=check_duplicated_query.format(
            column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )

    # ======== #
    #  PRICES  #
    # ======== #
    table = "prices"
    primary_key = "prices_id"
    # task to count rows of songplays
    check_rows_prices = DataQualityOperator(
        task_id=f"check_rows_{table}",
        table=table,
        dag=dag,
        sql_query=check_rows_query.format(table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to count nulls of userid
    check_pk_prices = DataQualityOperator(
        task_id=f"check_pk_{table}",
        table=table,
        dag=dag,
        sql_query=check_null_query.format(column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )

    # create DAG graph
    check_rows_cards >> check_pk_cards >> check_duplicated_cards
    check_rows_sets >> check_pk_sets >> check_duplicated_sets
    check_rows_artists >> check_pk_artists >> check_duplicated_artists
    check_rows_time >> check_pk_time >> check_duplicated_time
    check_rows_prices >> check_pk_prices

    return dag
