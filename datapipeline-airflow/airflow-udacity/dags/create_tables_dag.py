from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import os
from airflow import DAG
import logging


def parse_sql(file_path):
    """
    parses sql file into a list of statement
    param:
        file_path: path of sql file 
    """
    create_tables = open(file_path, 'r')
    sql_file = create_tables.read()
    sql_stmts =list(sql_file.split(';'))
    return sql_stmts


def create_tables():
    """
    creates tables in redshift database
    
    """
    redshift = PostgresHook(postgres_conn_id='redshift_conn_id')
    stmts = parse_sql(file_path='/home/workspace/airflow/create_tables.sql' )
    for  idx, stmt in enumerate(stmts):

      try:
        logging.info(f'running query {idx+1}')
        redshift.run(stmt)
      except:
        pass

    return(f'all tables now available in public schema')    


default_args = {
    'owner': 'salmanshahzad',
    'start_date': datetime(2020, 5, 17),
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG('create_tables',
          default_args=default_args,
          description='create tables in redshift db',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

create_tables_task = PythonOperator(

  task_id = 'create_tables',
  dag= dag,
  python_callable= create_tables

)

end_task = DummyOperator(task_id='stop_execution',  dag=dag)
start_operator >> create_tables_task >> end_task