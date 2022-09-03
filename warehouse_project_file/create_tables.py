"""
This Python module is responsible for creating empty tables in the database.
"""
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This functions allows to drop tables before creating the new table. It  iterates through the list of
    quesries drop_table_queries to drop 7 tables of the project.
    cur: cursor object
    conn: connection object
    return: None
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This functions allows to create empty tables before creating the new table. It  iterates through the list of
    quesries create_table_queries to create 7 tables of the project.
    cur: cursor object
    conn: connection object
    return: None
    """
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    This functions drop and creates tables. It creates a connections to the database and closes it once all operations
    are done.
    return: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()