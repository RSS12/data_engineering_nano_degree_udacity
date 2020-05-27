import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
       This function extract song and artist information from json files 
        and insert them in song and artist table
    Args: 
        param cur: connection to database
        param filepath: path to json data files
    """
    # open song file
    df = pd.read_json(filepath,lines= True)
    
    # insert artist record 
    artist_data =   df[['artist_id','artist_name','artist_location',\
                             'artist_latitude','artist_longitude']].values[0].tolist()
    
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    


def process_log_file(cur, filepath):
    """
       This function extract time, user and songplays information from json files 
       and insert them in song and artist table.
    Args:    
        param cur: connection to database
        param filepath: path to json data files
    """
    
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df =  df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = df['ts']
    
    #converting timestamp column to datetime with pandas timestamp function
    time_stamp = pd.to_datetime(t, unit='ms')
    
    # insert time data records
    time_data = list((t,pd.to_datetime(t, unit='ms'),time_stamp.dt.hour,
                      time_stamp.dt.day,time_stamp.dt.week,
                      time_stamp.dt.month,time_stamp.dt.year,time_stamp.dt.weekday ))
    
    column_labels = ['time_id','timestamp', 'hour','day','week','month','year','dayofweek'] 
    
    time_df =  pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId,\
                         row.location, row.userAgent)
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur,conn,filepath, func):
    
    
    """
    this function process data by calling other processing functions defined
    
    Args:
        param conn: connection to database
        param cur:  database connection curr. 
        param filepath: file path of json files
        param func: function to execute
    
    """
    # get all files matching extension from directory
   
    all_files = []
    
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    main function to call all the functions declaired 
    connection to the database is also declared in this function
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()