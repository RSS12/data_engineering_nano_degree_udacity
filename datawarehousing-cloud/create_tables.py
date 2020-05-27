import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    drop staging and data warehousing dimensions and fact tables in sparkify database.
    it loops through list of queries  "drop_table_queries" and execute them.
        
        Args: 
        param conn: connection to database
        param cur:  database connection curr.
    
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create staging and data warehousing dimensions and fact tables in sparkify database.
    it loops through list of queries  "create_table_queries" and execute them.
        
        Args: 
        param conn: connection to database
        param cur:  database connection curr.
    
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    It reads config file for Redshift database connection properties and connect to it.
    After connecting, it runs drop_tables and create_tables functions and closes connection
    to the database.
    
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