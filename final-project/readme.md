## Capstone-Project
### Scope

1. I am using here US Immigration data to provide analysis on Immigration rate from different countries 
2. Only using Immigration data comming to US with Air
3. US states with most high rates in accordance with population density
4. From the Data we will be able to do analysis on Immigration rate of different countries and cities
5. Which states/cities airport are used mostly

### Technologies used
1. Spark  to read data and do ETL and store this Data into S3
2. From S3 we can use Airflow to load data into Redshift 
3. Choice of redshift is made because of managed service from AWS


### Architecture
read SAS data --> raw data
raw data -->staging 
Stagging to transformed data
we can also enrich the data  from mapping tables and then store it 
Airflow >> Load Data to S3 with cronjob schedule
Airflow >> copy data to Redshift tables
Airflow create data checks

### End Result
Single source of truth
Provide some usefull analysis on Immigration data

### ERD 
![Data Model](udacity_capstone_new.png)
