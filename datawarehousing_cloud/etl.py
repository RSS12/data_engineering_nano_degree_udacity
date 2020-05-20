import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads data into  staging tables.
    It loops through query list "copy_table_queries",which reads data from S3 and load data into
    destination tables in redshift.
        
        Args: 
        param conn: connection to database
        param cur:  database connection curr.
    
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function loads data into  dimension and fact tables.
    It loops through query list "insert_table_queries",which reads data from staging tables and load it into
    destination tables in sparkify database.
        
        Args: 
        param conn: connection to database
        param cur:  database connection curr.
    
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    It reads config file for Redshift database connection properties and connect to it.
    After connecting, it runs load_staging_tables and insert_tables functions and closes connection
    to the database.
    
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