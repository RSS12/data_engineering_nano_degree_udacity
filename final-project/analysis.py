
import pandas as pd
import json
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
import  analysis_sql
import transformations_sql

spark = SparkSession.builder.\
config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
.enableHiveSupport().getOrCreate()

#read all data from transformation layer and create temp tables to  perform Analysis
df_facts = spark.read.parquet("transformation/facts_immigration/_year=2016/_month=4/*.parquet")
df_country_lookups = spark.read.parquet("transformation/mapping_tables/country_mapping/*.parquet")
df_state = spark.read.parquet("transformation/mapping_tables/state_of_addr_mapping/")

df_facts.createOrReplaceTempView("ds_facts")
df_country_lookups.createOrReplaceTempView('ds_countries')
df_state.createOrReplaceTempView('ds_state')


## Immigration rate per US state

immigration_rate_us_states = spark.sql(analysis_sql.immigration_rate_us_states)

try:
    transformations_sql.check_dataframe_empty(immigration_rate_us_states)
    print("data quality check passed")
     
    immigration_rate_us_states.withColumn('_year',immigration_rate_us_states.year).\
                                withColumn('_month',immigration_rate_us_states.month).\
                                    write.partitionBy("_year","_month").mode('overwrite').\
                                        parquet('analysis/immigration_rate_us_states')
   
    print("successfully write data to analysis layer")
    
except Exception as e:
        print(f"Data quality check failed: {e}")