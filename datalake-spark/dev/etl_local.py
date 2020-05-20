import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format



config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This functions reads song data from s3 bucket and transforms it 
    to create songs and artist tables
    tables created are again written to S3 in parquet file format
    
        param:
            spark: spark session
            input_date: local path for songs data
            output_data:local path for transformed data
    
    """
    # get filepath to song data file
    song_data = input_data +'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)
    
    #Register spark tempTable to run sql queries
    df.createOrReplaceTempView('songs_dataset')

    # extract columns to create songs table
    songs_table =  spark.sql( """
               
                            SELECT DISTINCT 
                            song_id,
                            title,
                            artist_id, 
                            year,
                            duration

                        FROM songs_dataset 

                        where song_id IS NOT NULL

                    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table  = spark.sql("""
                            SELECT DISTINCT 
                            artist_id, 
                            artist_name,
                            artist_location,
                            artist_latitude,
                            artist_longitude
                            FROM songs_dataset
                            WHERE artist_id IS NOT NULL
                        """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+'artists/')


def process_log_data(spark, input_data, output_data):
    
    """
    This functions reads song data from s3 bucket and transforms it 
    to create user , time and songplays table
    tables created are again written to S3 in parquet file format
    
        param:
            spark: spark session
            input_date:  local path for songs data
            output_data: local path for transformed data
    
    """
    # get filepath to log data file
    #log_data = input_data + 'log_data/*/*/*.json'
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    df.createOrReplaceTempView('log_dataset')

    # extract columns for users table    
    users_table = spark.sql("""
                        SELECT DISTINCT 
                        userId as user_id, 
                        firstName as first_name,
                        lastName as last_name,
                        gender as gender,
                        level as level
                        FROM log_dataset 
                        WHERE userId IS NOT NULL
                        
                    """)
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = spark.sql("""


                with A as (
                SELECT to_timestamp(ts/1000) as ts
                                        FROM log_dataset 
                                        WHERE ts IS NOT NULL

                )

                SELECT 
                    A.ts as start_time,
                    hour(A.ts) as hour,
                    dayofmonth(A.ts) as day,
                    weekofyear(A.ts) as week,
                    month(A.ts) as month,
                    year(A.ts) as year,
                    dayofweek(A.ts) as weekday

                FROM A

    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.limit(100).write.mode("overwrite").partitionBy("year", "month").parquet(output_data+'time_table/')


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT 
                                monotonically_increasing_id() as songplay_id,
                                to_timestamp(logD.ts/1000) as start_time,
                                month(to_timestamp(logD.ts/1000)) as month,
                                year(to_timestamp(logD.ts/1000)) as year,
                                logD.userId as user_id,
                                logD.level as level,
                                songD.song_id as song_id,
                                songD.artist_id as artist_id,
                                logD.sessionId as session_id,
                                logD.location as location,
                                logD.userAgent as user_agent
                                FROM log_dataset logD
                                JOIN songs_dataset songD
                                on 
                                logD.artist = songD.artist_name
                                                and logD.song = songD.title
                            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "./data/"
    output_data = "./data/dw/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
