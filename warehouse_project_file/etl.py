"""
This Python module is responsible for loading data from s3 buckets to the database.
It applies necessary transformations on the data.
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This functions allows to load data to staging tables. It uses COPY commands to load the datasets from s3 buckets.
    cur: cursor object
    conn: connection object
    return: None
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This functions allows to load data to the star schema tables.
     It uses INSERT commands to load the datasets from the staging tables.
    cur: cursor object
    conn: connection object
    return: None
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    This functions extract data from s3 buckets to the staging tables. Then, it takes data from the staging tables
    and load the data to the star schema tables..
    It creates a connections to the database and closes it once all operations  are done.
    return: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()