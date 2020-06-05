import pandas as pd
import json
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
import transformations_sql

spark = SparkSession.builder.\
config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
.enableHiveSupport().getOrCreate()

# reading raw data from  the source and saving it as parquet in staging layer
df_sas=spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-#2016/i94_apr16_sub.sas7bdat')

df_sas = df_sas.withColumn("year",df_sas.i94yr.cast('integer'))\
               .withColumn("month",df_sas.i94mon.cast('integer'))

df_sas.write.mode("overwrite").partitionBy("year","month").parquet('raw/facts_air_immigration')

                                                                             
df_parq = spark.read.parquet('raw/facts_air_immigration/year=*/month=*/*.parquet')

#filtering for immigrants who came to US via air
df_parq_air =  df_parq.filter(df_parq.i94mode == 1)



# creating temp table to query dataframe
df_parq_air.createOrReplaceTempView('dataset')

# staging personal data 
dim_personal_stg = spark.sql(transformations_sql.dim_personal_stg)

# writing this data to staging layer
dim_personal_stg.write.mode('overwrite').parquet('staging/dim_personal/')


# stage facts
facts_staging = spark.sql(transformations_sql.facts_staging)
facts_staging.write.partitionBy("_year","_month").mode('overwrite').parquet("staging/facts_air_immigration")

# transformation step

# dim personal
personal_stg_read =spark.read.format("parquet").load('staging/dim_personal/') 



#create temp view
personal_stg_read.createOrReplaceTempView("ds_personal_stg")


print("removing all ids having duplicate records")

#In this step we can also keep single latest record of id using a valid date column. In our case , data doesnot compliy

dim_personal= spark.sql(transformations_sql.dim_personal)

#writing this dataset to transformation layer
dim_personal.write.mode("overwrite").parquet('./transformation/dim_personal')         

#creating temp table
dim_personal.createOrReplaceTempView('ds_personal')   


# removing all unknown ids from  fact dataset
fact_stg_read = spark.read.parquet("staging/facts_air_immigration/_year=*/_month=*/*.parquet")

#check if data frame is not empty
print("performing check")
try:
    transformations_sql.check_dataframe_empty(fact_stg_read)
    transformations_sql.check_dataframe_empty(personal_stg_read)

    fact_stg_read.createOrReplaceTempView("dataset_stg")

    fact_us_immigrations= spark.sql(transformations_sql.fact_us_immigrations).drop_duplicates()      

    fact_us_immigrations.write.mode("overwrite").partitionBy('_year','_month').parquet('transformation/facts_immigration')
    
except Exception as e:
    print("data quality check failed with exception:\n {e}")    










