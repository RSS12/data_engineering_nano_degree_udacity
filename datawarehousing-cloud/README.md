# SPARKIFY CLOUD DATAWAREHOUSING USING AWS INFRASTRUCTURE

## Introduction
Sparkify, a startup company have grown their user base and now wants to build a data warehouse to better analyse and understand
user behavior and make decisions to make their product better.

They want to design a Datawarehouse using  Cloud Infrastructure and have acquired AWS webservices.
Data is stored in S3 buckets in json format.


## Project Description
In this project , I have used python and redshift database from AWS  to build an etl pipe line that extracts data from 
aws S3 storage and stage them in staging tables.The data from staging tables was extracted, transformed and loaded into 
datawarehouse dimensions and fact tables.
Data files are in json format.


## Project Design
- Staging tables for event and songs data

- Schema for sparkify data warehouse.
    - Fact Table
         - songplays records in log data associated with song plays
    - Dimension Tables 
        - users in the app
        - songs in music database
        - artists in music database
        - time: timestamps of records in songplays broken down into specific units



## Database Design

- Staging tables are created using 'auto' distribution. 

- Data warehousing tables are then loaded using staging tables.
    
    - Distribution style 'All' was used on dimension tables.So when we want to run complex queries all dimensions are available
    on all nodes. This will make analytical queries faster.
    
    - Distribution style 'EVEN' was used for fact table. This table will grow quite fast and I believe 'auto' or even style        distributin is a good fit.
    
    - Sort key was used on dim_user table, So the data is stored in sorted way in dim user table
    


## ETL Scripts

- running create_tables.py will drop and create new tables.
- etl.py script perform etl , it loads data into staging tables and all data warehousing tables including fact and dimensions.
- dwh.cfg file contains configs for S3 locations for staging data and redshift connection properties.
- I have used local jupyter notebook locally to launch redshift cluster and create IAM role.I also verfied the data using Redshift console.