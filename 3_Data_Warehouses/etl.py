import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Runs all COPY QUERIES
    :param cur  :database cursor
    :param conn :database connector
    '''
    for query in copy_table_queries:
        set_schema = query[0]
        copy_table = query[1]
        cur.execute(set_schema)
        cur.execute(copy_table)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Runs all INSERT QUERIES
    :param cur  :database cursor
    :param conn :database connector
    '''
    for query in insert_table_queries:
        set_schema = query[0]
        insert_table = query[1]
        cur.execute(set_schema)
        cur.execute(insert_table)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()