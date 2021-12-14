from airflow import DAG
from airflow.operators import DataQualityOperator


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

    table = "songplays"
    primary_key = "songplay_id"

    # task to count rows of songplays
    check_rows_songplays = DataQualityOperator(
        task_id=f"check_rows_{table}",
        table=table,
        dag=dag,
        sql_query=check_rows_query.format(table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to count nulls of songplay_id
    check_pk_songplays = DataQualityOperator(
        task_id=f"check_pk_{table}",
        table=table,
        dag=dag,
        sql_query=check_null_query.format(column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )

    table = "users"
    primary_key = "userid"

    # task to count rows of users
    check_rows_users = DataQualityOperator(
        task_id=f"check_rows_{table}",
        table=table,
        dag=dag,
        sql_query=check_rows_query.format(table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to count nulls of userid
    check_pk_users = DataQualityOperator(
        task_id=f"check_pk_{table}",
        table=table,
        dag=dag,
        sql_query=check_null_query.format(column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to check duplicated primary key on userid
    check_duplicated_users = DataQualityOperator(
        task_id=f"check_duplicated_{table}",
        table=table,
        dag=dag,
        sql_query=check_duplicated_query.format(
            column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )

    table = "songs"
    primary_key = "song_id"
    foreign_key = "artist_id"

    # task to count rows of songs
    check_rows_songs = DataQualityOperator(
        task_id=f"check_rows_{table}",
        table=table,
        dag=dag,
        sql_query=check_rows_query.format(table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to count nulls of song_id
    check_pk_songs = DataQualityOperator(
        task_id=f"check_pk_{table}",
        table=table,
        dag=dag,
        sql_query=check_null_query.format(column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to count nulls of artist_id
    check_fk_songs = DataQualityOperator(
        task_id=f"check_fk_{table}",
        table=table,
        dag=dag,
        sql_query=check_null_query.format(column=foreign_key, table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to check duplicated primary key on song_id
    check_duplicated_songs = DataQualityOperator(
        task_id=f"check_duplicated_{table}",
        table=table,
        dag=dag,
        sql_query=check_duplicated_query.format(
            column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )

    table = "artists"
    primary_key = "artist_id"

    # task to count rows of artists
    check_rows_artists = DataQualityOperator(
        task_id=f"check_rows_{table}",
        table=table,
        dag=dag,
        sql_query=check_rows_query.format(table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to count nulls of artist_id
    check_pk_artists = DataQualityOperator(
        task_id=f"check_pk_{table}",
        table=table,
        dag=dag,
        sql_query=check_null_query.format(column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to check duplicated primary key on artist_id
    check_duplicated_artists = DataQualityOperator(
        task_id=f"check_duplicated_{table}",
        table=table,
        dag=dag,
        sql_query=check_duplicated_query.format(
            column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )

    table = "time"
    primary_key = "start_time"

    # task to count rows of time
    check_rows_time = DataQualityOperator(
        task_id=f"check_rows_{table}",
        table=table,
        dag=dag,
        sql_query=check_rows_query.format(table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to count nulls of start_time
    check_pk_time = DataQualityOperator(
        task_id=f"check_pk_{table}",
        table=table,
        dag=dag,
        sql_query=check_null_query.format(column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )
    # task to check duplicated primary key on start_time
    check_duplicated_time = DataQualityOperator(
        task_id=f"check_duplicated_{table}",
        table=table,
        dag=dag,
        sql_query=check_duplicated_query.format(
            column=primary_key, table=table),
        redshift_conn_id=redshift_conn_id
    )

    # create DAG graph
    check_rows_songplays >> check_pk_songplays
    check_rows_users >> check_pk_users >> check_duplicated_users
    check_rows_songs >> [check_pk_songs,
                         check_fk_songs] >> check_duplicated_songs
    check_rows_artists >> check_pk_artists >> check_duplicated_artists
    check_rows_time >> check_pk_time >> check_duplicated_time

    return dag
