import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# S3 bucket (for staging event and song data) and AWS resources(IAM role arn)

LOG_DATA_PATH = config['S3']['LOG_DATA']
SONG_DATA_PATH = config['S3']['SONG_DATA']
LOG_JSON_PATH = config['S3']['LOG_JSONPATH']
IAM_ROLE_ARN = config['IAM_ROLE']['ARN']

# DROP TABLES

#staging tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events "
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs "

# dw tables
songplay_table_drop = "DROP TABLE IF EXISTS     fact_songplay"
user_table_drop     = "DROP TABLE IF EXISTS     dim_user "
song_table_drop     = "DROP TABLE IF EXISTS     dim_song"
artist_table_drop   = "DROP TABLE IF EXISTS     dim_artist"
time_table_drop     = "DROP TABLE IF EXISTS     dim_time "



# CREATE TABLES
# 'auto' distribution is selected when nothing is declared 

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist          VARCHAR,
    auth            VARCHAR, 
    firstName       VARCHAR,
    gender          VARCHAR(1),   
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR, 
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    sessionId       VARCHAR,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR,
    userId          INTEGER
);

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
                    num_songs INTEGER,
                    artist_id VARCHAR,
                    artist_latitude VARCHAR,
                    artist_longitude VARCHAR ,
                    artist_location VARCHAR ,
                    artist_name VARCHAR, 
                    song_id VARCHAR,
                    title VARCHAR,
                    duration FLOAT,
                    year INTEGER
                        );
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay (
                        songplay_id      INTEGER IDENTITY(0,1) PRIMARY KEY sortkey ,
                        start_time       TIMESTAMP,
                        user_id          INTEGER,
                        level            VARCHAR,
                        song_id          VARCHAR,
                        artist_id        VARCHAR,
                        session_id       VARCHAR,
                        location         VARCHAR,
                        userAgent        VARCHAR
                        
                        )
diststyle even;                        
""")




user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user (
                user_id    INTEGER PRIMARY KEY  ,
                first_name VARCHAR,
                last_name  VARCHAR,
                gender  VARCHAR(1),
                level   VARCHAR )
diststyle all;

""")


song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song (
               song_id VARCHAR PRIMARY KEY, 
               title VARCHAR,
               artist_id VARCHAR,
               year INTEGER,
               duration FLOAT
               )
diststyle all;

""")




artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist (
                artist_id  VARCHAR PRIMARY KEY, 
                name VARCHAR,
                location VARCHAR, 
                lattitude VARCHAR, 
                longitude VARCHAR            
                        )
diststyle all;

""")


time_table_create = ("""

CREATE TABLE IF NOT EXISTS dim_time (
           start_time  TIMESTAMP PRIMARY KEY,
           hour        INTEGER,
           day         INTEGER,
           week        INTEGER,
           month       INTEGER,
           year        INTEGER,
           weekday     INTEGER
                        )
diststyle all;                        

""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};

""").format(LOG_DATA_PATH,IAM_ROLE_ARN,LOG_JSON_PATH)


staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
compupdate OFF region 'us-west-2'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON  'auto' ;
""").format(SONG_DATA_PATH,IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay(start_time,user_id,level ,song_id , artist_id ,session_id ,location ,userAgent)

SELECT Distinct
    e.ts,
    e.userId as user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId  as session_id,
    e.location,
    e.userAgent

FROM staging_events as e

JOIN staging_songs as s
ON
e.song = s.title AND
e.artist =s.artist_name 

where e.page = 'NextSong';
""")

user_table_insert = ("""

INSERT INTO dim_user(user_id,first_name,last_name,gender,level)
SELECT DISTINCT
    userId,
    firstName,
    lastName,
    gender,
    level

FROM staging_events
WHERE userId IS NOT NULL;
""")

song_table_insert = ("""

INSERT INTO dim_song(song_id,title,artist_id,year,duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
    
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""

INSERT INTO dim_artist( artist_id,name,location,lattitude,longitude)
SELECT DISTINCT
   artist_id,
   artist_name,
   artist_location,
   artist_latitude,
   artist_longitude

FROM staging_songs
WHERE artist_id IS NOT NULL
;

""")

time_table_insert = ("""
INSERT INTO dim_time(start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT
    ts ,
    EXTRACT('hour' from ts) as hour,
    EXTRACT('day' from ts) as day,
    EXTRACT('week' from ts) as week,
    EXTRACT('month' from ts) as month,
    EXTRACT('year' from ts) as year,
    EXTRACT('weekday' from ts) as weekday
    
FROM staging_events
WHERE ts IS NOT NULL;   

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
