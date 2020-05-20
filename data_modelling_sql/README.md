# SPARKIFY DATAWAREHOUSE

## Introduction
Sparkify, a startup company wants to analyse users activity on their music streaming platform.
Which songs users are listening to in particular.

They want to design a Datawarehouse using  Postgres Database.
Data modeling technique used in this project is called Star schema

## Project Description
In this project , I have used python and sql to build etl pipe line that extracts data from  two data dirs and load that data into Postgres tables, designed in a star schema.
Data files are in json format.


## Project Design

- Schema for Song Play Analysis
    - Fact Table
         - songplays records in log data associated with song plays
    - Dimension Tables 
        - users in the app
        - songs in music database
        - artists in music database
        - time: timestamps of records in songplays broken down into specific units




## Database Design

As foreign key constraints are also implemented in the Database design , Data should be loaded in an order

- First Dimensions with no foreign keys should be loaded
- Fact table, in this case song plays table should be loaded after loading Dimension tables.


## ETL Scripts

- running create_tables.py will drop and create new tables.
- I have used jupyter notebook for development.
- After confirmation that the process is running fine,  I have transfered the code to etl.py
- test.ipynb can be used to see the results of etl process 