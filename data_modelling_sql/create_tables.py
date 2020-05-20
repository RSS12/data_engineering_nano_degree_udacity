import psycopg2
import sys

sys.path.append('./data_modelling_sql')
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=test user=postgres password=salman")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")


    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=salman")
    cur = conn.cursor()
    
    return cur, conn

def create_schema():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=salman")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS udacity")
    cur.execute("CREATE  SCHEMA udacity ")
    return ("schema created successfully")


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    #creating schema , I do not want to work with default public schema
    create_schema()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()