# pyspark sql to perform some transformations


dim_personal_stg = """
                SELECT DISTINCT 
                admnum as id, 
                gender,
                biryear as birth_year,
                i94mode as entry_mode ,
                i94visa as visa_category,
                i94cit as origin_country
                FROM dataset 
                
                """


dim_personal= """
                SELECT * from ds_personal_stg 
                
                WHERE id not in (
                        SELECT id from ds_personal_stg group by id having count(1)>1 
                         ) 
              """




facts_staging = """
 SELECT 
 cicid,
 cast(i94yr as INTEGER) as  _year,
 cast(i94mon as INTEGER) as _month,
 cast(i94yr as INTEGER) as year,
 cast(i94mon as INTEGER) as month,
 admnum  as  entry_number,
 i94cit as origin_country_code,
 i94port as arrival_port_code,
 i94addr as sas_arrival_date,
 depdate as sas_departure_date,
 i94addr as  state_code,
 count,
 entdepd as departue_status,
 entdepu as departure_status_update
 
 FROM dataset
 
"""



fact_us_immigrations= """
        SELECT 
        A.cicid ,
        A.year,
        A.month,
        A.year as _year,
        A.month as _month,
        B.id as entry_number,
        A.origin_country_code,
        A.arrival_port_code, 
        A.sas_arrival_date,
        A.sas_departure_date,
        A.state_code,
        A.count,
        A.departue_status,
        A.departure_status_update

        FROM dataset_stg as A

        JOIN
        ds_personal as B
        on A.entry_number = B.id

        """

def sas_date_convert(sas_date):
    """
    convert sas date format to date
    sas_date: sas date format
    
    """
    return pd.to_timedelta(sas_date, unit='D') + pd.Timestamp('1960-1-1') 


def check_dataframe_empty(df):
    
    """
    checks if data frame is empty
    params:
    df: spark dataframe
    ruturn shape of 
    """
    
    result = df.count()
    if result == 0:
        print("Data quality check failed forwith zero records")
    else:
        print("Data quality check passed")
        
    return (f"{result} rows in dataframe")    
        



