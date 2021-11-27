import configparser
import logging

import psycopg2

from sql_queries import copy_table_queries, insert_table_queries

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig()


def load_staging_tables(cur, conn):
    """
    Executes the copy statements to create the staging tables.
    """
    logger.info("Staging Tables...")
    for query in copy_table_queries:
        try:
            logger.debug(query)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logger.error(e)


def insert_tables(cur, conn):
    """
    Executes the insert statements to populate final tables from staging.
    """
    logger.info("Inserting Tables...")
    for query in insert_table_queries:
        try:
            logger.debug(query)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logger.error(e)


def main():
    """
    - Gets the configuration parametrs from dwh.cfg

    - Establishes connection with the Redshift database and gets
    cursor to it.

    - Copy the staging tables to Redshift.

    - Insert data from staging tables in the final tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".
                            format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
