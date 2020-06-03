## Capstone-Project
### Step 1: Scope the Project and Gather Data

#### Scope 

**In this project I want to anylse immigration rate of different US states**

**Solution**:

data-lake on  cloud storage using apache spark.

data-lake involves 3 layers

- **stagging** 
    - Source data is read and loaded as parquet in the stagging layer using partition on year and month
- **transformation**
    - Data transformation is performed on the stagged data and saved as parquet as partitioned data
- **analysis**
    - From transformed data, we extract data and store it as dimensions and facts. This analysis data then can be loaded into AWS Redshift Datawarehouse. Which scales according to the load.


[**Data Model**] : Data model image


**Technologies**:
1. AWS S3 for Data Lake
2. Spark (can be used on emr cluster/ aws glue) 
3. AWS Redshift (This is used as it can handle  big data sets and number of concurrent users upto 500)
4. We can schedule etl jobs by different approaches namely:
    - airflow 
    - cron job
    - app flow from aws
    - Step functions
    
#### Dataset used
- I94 Immigration Data
- us-cities-demographic
- mapping tables

Explain what you plan to do in the project in more detail. What data do you use? What is your end solution look like? What tools did you use? etc>

#### Describe and Gather Data 
* I94 Immigration Data: This data comes from the US National Tourism and Trade Office
* us-cities-demographic dataset: This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
* mapping tables: Look up tables for data used in immigration dataset.

### ERD 
![Data Model](udacity_capstone_new.png)
