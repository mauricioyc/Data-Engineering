import configparser
import logging

import psycopg2

from sql_queries import create_table_queries, drop_table_queries

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig()


def drop_tables(cur, conn):
    """
    Executes the queries to drop Redshift tables.
    """
    logger.info("Dropping Tables...")
    for query in drop_table_queries:
        try:
            logger.debug(query)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logger.error(e)


def create_tables(cur, conn):
    """
    Executes the queries to create Redshift tables.
    """
    logger.info("Creating Tables...")
    for query in create_table_queries:
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

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, closes the connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".
                            format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
