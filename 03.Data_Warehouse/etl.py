import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Add data from S3 to staging tables
    :param cur: postgresql database cursor
    :param conn: postgresql database connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables to fact and dimensional tables
    :param cur: postgresql database cursor
    :param conn: postgresql database connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connecting database and processing and loading songs and logs data. 
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