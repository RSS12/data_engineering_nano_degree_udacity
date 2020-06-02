"""
This file reads data from different data sources (json,csv)
perform some transformations and stores them using pyspark

All files created from this module is in overwrite mode.
Everytime we run this file creates new mapping tables and dimensions 

Dimensions:[dim_date, dim_us_demographics]
mapping_tables:['country_mapping','mode_of_entry_mapping','port_entry_mapping','state_of_addr_mapping','visa_cat_mapping']

"""


import pandas as pd
import json


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

with open('immigration_mapping.json', 'r') as mp:
    matching_labels = json.load(mp)
    mp.close()

    
def convert_dict_to_df(dict_to_convert)->pd.DataFrame:
    """
    converts dict to pandas dataframe
    """
    df = pd.DataFrame(data=[*dict_to_convert.values()],index=dict_to_convert.keys())
    df.reset_index(inplace=True)
    return df

def save_parquet(df,path,file_name):
    df_spark = spark.createDataFrame(df)
    df_spark.coalesce(1)
    df_spark.write.mode('overwrite').parquet(f'{path}/{file_name}')
    return (f'{file_name} stored at: {path}')



print('converting dict to mapping dataframes')
print('='*50)

country_mapping       = convert_dict_to_df(matching_labels['country_mapping'])
mode_of_entry_mapping = convert_dict_to_df(matching_labels['modeOfEntry_mapping'])
port_entry_mapping    = convert_dict_to_df(matching_labels['portOfEntry_mapping'])
state_of_addr_mapping = convert_dict_to_df(matching_labels['stateOfAddr_mapping'])
visa_cat_mapping      = convert_dict_to_df(matching_labels['visaCat_mapping'])

country_mapping.columns       = ['code','country']
mode_of_entry_mapping.columns = ['id','entry_mode']
port_entry_mapping.columns    = ['id','entry_port']
state_of_addr_mapping.columns = ['id','state_name']
visa_cat_mapping.columns      = ['id','visa_type']
port_entry_mapping[['city','state','state_1']] = port_entry_mapping['entry_port'].str.split(',',expand= True)

#tables to create list
mapping_tables_list = ['country_mapping','mode_of_entry_mapping','port_entry_mapping','state_of_addr_mapping','visa_cat_mapping']

#data frame list
df_list = [country_mapping,mode_of_entry_mapping,port_entry_mapping,state_of_addr_mapping,visa_cat_mapping] 

#creating dict out of both lists to iterate through it when creating mapping tables
dict_table_df = dict(list(zip(mapping_tables_list,df_list)))

for k,v  in dict_table_df.items():
    print("writing data is in overwrite mode")
    save_parquet(df=dict_table_df[k] , path ='./data-transformed/mapping_tables',file_name= k)
    

## Create date_dim

print("creating dim date")
print('='*50)

def create_date_table(start='2012-01-01', end='2030-12-31'):
    """
    create date dimension
    """
    df = pd.DataFrame({"Date": pd.date_range(start, end)})
    df["Date_key"] = df["Date"].dt.strftime('%Y%m%d')
    df["Day"] = df.Date.dt.weekday_name
    df["Week"] = df.Date.dt.weekofyear
    df["Quarter"] = df.Date.dt.quarter
    df["Year"] = df.Date.dt.year
    df["Year_half"] = (df.Quarter + 1) // 2
    return df

dim_date =  create_date_table()

save_parquet(df=dim_date,path ='./data-transformed/mapping_tables',file_name= 'dim_date')


print("creating dim_us_demographics")
print('='*50)
## create dim_us_demographics
dim_us_demographics = spark.read.format("csv").load('us-cities-demographics.csv',header=True,sep=';')

## renaming columns to write into parquet formats
dim_us_demographics = dim_us_demographics.\
                                withColumnRenamed('City','city').\
                                withColumnRenamed('State','state').\
                                withColumnRenamed('Median Age','median_age').\
                                withColumnRenamed('Female Population','females').\
                                withColumnRenamed('Male Population','males').\
                                withColumnRenamed('Total Population','total_population').\
                                withColumnRenamed('Average Household Size','average_household').\
                                withColumnRenamed('State Code','state_code').\
                                withColumnRenamed('State Code','state_code').\
                                withColumnRenamed('Race','race').\
                                withColumnRenamed('Count','house_holds').\
                                withColumnRenamed('Number of Veterans','veterans').\
                                write.mode('overwrite').parquet('./data-transformed/dim_us_demographics')
                                


