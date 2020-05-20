# DROP TABLES

songplay_table_drop = "DROP TABLE IF  EXISTS udacity.songlays;"
user_table_drop = "DROP TABLE IF  EXISTS udacity.users;"
song_table_drop = "DROP TABLE IF  EXISTS udacity.songs;"
artist_table_drop = "DROP TABLE IF  EXISTS udacity.artists;"
time_table_drop = "DROP TABLE IF  EXISTS udacity.time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS udacity.songplays (
    songplay_id serial  Primary key,
    time_id text  REFERENCES udacity.time(time_id),
    user_id int  REFERENCES udacity.users(user_id),
    level text  ,
    song_id text REFERENCES udacity.songs(song_id),
    artist_id text  REFERENCES udacity.artists(artist_id),
    session_id text ,
    location text ,
    user_agent text
    );
    
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS udacity.users (
    user_id int PRIMARY KEY,
    first_name text ,
    last_name text, 
    gender varchar(1),
    level text
); """)

song_table_create = ("""
CREATE TABLE IF NOT EXISTS udacity.songs  (
    song_id text PRIMARY KEY,
    title text ,
    artist_id text REFERENCES udacity.artists(artist_id),
    year int ,
    duration text);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS udacity.artists  (
    artist_id text PRIMARY KEY,
    name text ,
    location text ,
    latitude text ,
    longitude text );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS udacity.time (        
        time_id text PRIMARY KEY , 
        start_time  timestamp  ,
        hour int,
        day int, 
        week int,
        month int,
        year int, 
        weekday  int);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT  INTO  udacity.songplays ( songplay_id
                               , time_id
                               , user_id
                               , level
                               , song_id
                               , artist_id
                               , session_id
                               , location
                               , user_agent)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) 
ON CONFLICT DO NOTHING

""")

user_table_insert = (""" 
INSERT INTO udacity.users(
                        user_id
                       ,first_name
                       ,last_name
                       ,gender
                       ,level
)
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (user_id) DO UPDATE
SET level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO udacity.songs(
                        song_id
                        ,title
                        ,artist_id 
                        ,year
                        ,duration
)
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO udacity.artists(
                         artist_id
                        ,name
                        ,location
                        ,latitude
                        ,longitude
                        )
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (artist_id) DO NOTHING;

""")


time_table_insert = ("""
INSERT INTO udacity.time(
                         time_id
                         ,start_time
                         ,hour
                         ,day
                         ,week
                         ,month
                         ,year
                         ,weekday

)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (time_id) DO NOTHING ;
""")

# FIND SONGS

song_select = ("""
SELECT A.song_id ,
        A.artist_id
        FROM udacity.songs as A
join udacity.artists as B
on A.artist_id  = B.artist_id
Where A.title = %s   AND B.name=%s AND  cast(A.duration as float)=%s
""")

# QUERY LISTS

create_table_queries = [user_table_create,artist_table_create,song_table_create, time_table_create,songplay_table_create]
drop_table_queries = [ user_table_drop,artist_table_drop, song_table_drop, time_table_drop,songplay_table_drop]