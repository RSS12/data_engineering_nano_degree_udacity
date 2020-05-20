## Airflow Datapipeline

Data pipeline for loading data from S3 buckets into aws Redshift using apache Airflow


### Project Structure
.
└── airflow
    ├── create_tables.sql
    ├── dags
    │   ├── create_tables_dag.py
    │   ├── data_pipeline_dag.py
    │   └── __pycache__
    │       ├── create_tables.cpython-36.pyc
    │       ├── create_tables_dag.cpython-36.pyc
    │       ├── data_pipeline_dag.cpython-36.pyc
    │       └── udac_example_dag.cpython-36.pyc
    ├── plugins
    │   ├── helpers
    │   │   ├── __init__.py
    │   │   ├── __pycache__
    │   │   │   ├── __init__.cpython-36.pyc
    │   │   │   └── sql_queries.cpython-36.pyc
    │   │   └── sql_queries.py
    │   ├── __init__.py
    │   ├── operators
    │   │   ├── data_quality.py
    │   │   ├── __init__.py
    │   │   ├── load_dimension.py
    │   │   ├── load_fact.py
    │   │   ├── __pycache__
    │   │   │   ├── data_quality.cpython-36.pyc
    │   │   │   ├── __init__.cpython-36.pyc
    │   │   │   ├── load_dimension.cpython-36.pyc
    │   │   │   ├── load_fact.cpython-36.pyc
    │   │   │   └── stage_redshift.cpython-36.pyc
    │   │   └── stage_redshift.py
    │   └── __pycache__
    │       └── __init__.cpython-36.pyc
    └── readme.md