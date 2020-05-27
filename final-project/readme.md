## Capstone-Project
### Scope
1. I am using here US Immigration data to provide single source of truth to all users at US Immigrations
2. From the Data we will be able to do analysis on Immigration rate of different countries and cities
3. Which states/cities airport are used mostly

### Technologies used
1. Spark sql to read data and do ETL and store this Data into S3
2. From S3 we can use Airflow to load data into Redshift 
3. Choice of redshift is made because of managed service from AWS


### Architecture
local/S3 >> spark >> S3 
Airflow >> Load Data to S3 with cronjob schedule

### End Result
Single source of truth
Provide some usefull analysis on Immigration data


