import psycopg2
import pandas as pd
import numpy as np
import glob
import os
import sys
sys.path.append('data_modelling_sql')
from sql_queries import *
from sqlalchemy import create_engine
import io


def connect_db():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=salman")
    cur = conn.cursor()
    return conn,cur
## Sample data read

# log_file_data = pd.DataFrame()
# song_file_data = pd.DataFrame()
#
#
# log_data_path = './data_modelling_sql/data/log_data/'
# song_data_path = './data_modelling_sql/data/song_data/'
# # log_data_path = './data_modelling_sql/data/log_data/2018/11/'
#
# log_data_path_pattern = os.path.join(log_data_path,'*/*/*.json')
# song_data_path_pattern = os.path.join(song_data_path,'[A-Z]*/[A-Z]*/[A-Z]*/*.json')
#
# log_data_file_list = glob.glob(log_data_path_pattern)
# song_data_file_list = glob.glob(song_data_path_pattern)
#
#
# for files in log_data_file_list:
#     file_data =  pd.read_json(files,lines=True)
#     log_file_data = log_file_data.append(file_data,ignore_index=True)
#
# for files in song_data_file_list:
#     file_data =  pd.read_json(files,lines=True)
#     song_file_data = song_file_data.append(file_data,ignore_index=True)

#sample
# log_data = pd.read_json('./data_modelling_sql/data/log_data/2018/11/**.json',lines=True)

conn,cur = connect_db()

def get_files(file_path):
    all_files = []
    for root, dirs, files in os.walk(file_path):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


def get_df(file_path,df=pd.DataFrame()):
    files_list=get_files(file_path)

    for f in files_list:
        d = pd.read_json(f,lines=True)
        df = df.append(d)
    return df

df_log = get_df(file_path='data_modelling_sql/data/log_data')
df_log_filter = df_log[df_log['page']=='NextSong'].drop_duplicates()

df_songs = get_df(file_path='data_modelling_sql/data/song_data')


# songs Data
# dim_artist = df_songs[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].drop_duplicates()  # only uniques
dim_artist = df_songs[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].drop_duplicates()
dim_song = df_songs[['song_id','title','artist_id','year','duration']].drop_duplicates()

#log Data

dim_user = df_log_filter[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates().sort_values(by='firstName')


# ts_log_data =  pd.to_datetime(df_log_filter['ts'],unit='ms')


#Creating dim_time dimension

# ts_columns= ['time_id','start_time' ,'hour', 'day', 'week', 'month', 'year', 'weekday']
time_column= df_log_filter['ts']
# time_column= pd.DataFrame(df_log_filter['ts'])


ts = pd.to_datetime(time_column, unit='ms')

t = list((time_column,ts,ts.dt.hour,ts.dt.day,ts.dt.week,ts.dt.month,ts.dt.year,ts.dt.weekday ))
t_columns =  ['time_id','timestamp', 'hour','day','week','month','year','dayofweek']

dim_time = pd.DataFrame.from_dict(dict(zip(t_columns,t)))


## dim  songplays

# song_select_query = cur.execute(song_select)
#
# select_query_columns = []
#
# for c in cur.description:
#     select_query_columns.append(c[0])
#
# results_song_select = cur.fetchall()
#
# df_song_select = pd.DataFrame(results_song_select,columns=select_query_columns)
# df_song_select = pd.read_sql(song_select,con=conn)

# dim_songplays_prep = df_log_filter[['ts','userId','level','song','artist','sessionId','location','userAgent']]



for index, row in df_log_filter.iterrows():

    # get songid and artistid from song and artist tables
    cur.execute(song_select, (row.song, row.length,row.artist))
    results = cur.fetchone()

    if not results:
        song_id, artist_id = None,None
    else:
        song_id, artist_id = results

    # insert songplay record
    songplay_data = (index, row.ts, row.userId, row.level, song_id, artist_id, row.sessionId,\
                     row.location, row.userAgent)
    cur.execute(songplay_table_insert, songplay_data)
    conn.commit()





#pd.merge(left, right, how='left', on=['key1', 'key2'])
















#timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent

#
# ( songplay_id
#                                , time_id
#                                , user_id
#                                , level
#                                , song_id
#                                , artist_id
#                                , session_id
#                                , location
#                                , user_agent)


#dim_user.to_csv('./dim_user.csv')

# engine = create_engine('postgresql+psycopg2://postgres:salman@localhost:5432/sparkifydb')

#df.head(0).to_sql('table_name', engine, if_exists='replace',index=False) #truncates the table

# conn = engine.raw_connection()
# cur = conn.cursor()

# def bulk_insert_data(df,table_name:str):
#     """takes 2 args , df:DataFrame and table_name:already defined in Postgres and writes data to the table"""
#     output = io.StringIO()
#     df.to_csv(output, sep='\t', header=False, index=False)
#     output.seek(0)
#     contents = output.getvalue()
#     cur.copy_from(output, table_name, null="") # null values become ''
#     conn.commit()
#
#
#
# bulk_insert_data(dim_song,'udacity.artists')




def load_data_dim(dataframe,insert_query):
    """
    dataframe: pandas dataframe of dimension/fact being loaded
    insert_query: insert query to be used to load data with upsert logic implemented

    """
    try:

        for i , row in dataframe.iterrows():
            cur.execute(insert_query,list(row))
            conn.commit()
        return ('data loaded successfully')

    except Exception as e:
        print('insert error: ')
        return e







# Loading data

load_data_dim(dim_artist,artist_table_insert)
load_data_dim(dim_song,song_table_insert)
load_data_dim(dim_user,user_table_insert)
load_data_dim(dim_time,time_table_insert)



# def test(f,l):
#     print(f+' '+l)
#
#
# def process(f,l,fun):
#
#     return fun(f,l)
#
# process('salman','shahzad',fun=test)