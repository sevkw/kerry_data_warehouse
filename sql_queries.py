import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
region = config.get("S3", "REGION")
aws_arn = config.get("IAM_ROLE", "ARN")
log_data = config.get("S3", "LOG_DATA")
log_jsonpath = config.get("S3", "LOG_JSONPATH")
song_data = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fct_songplays;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length NUMERIC(10, 5),
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_location VARCHAR,
        artist_latitude VARCHAR,
        artist_longitude VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration NUMERIC,
        year SMALLINT
    )
""")

songplay_table_create = ("""
    CREATE TABLE fct_songplays (
        songplay_id VARCHAR PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INTEGER,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE dim_users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE dim_songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INTEGER,
        duration NUMERIC
    )
""")

artist_table_create = ("""
    CREATE TABLE dim_artists (
        artist_id VARCHAR PRIMARY KEY,
        artist_name VARCHAR,
        artist_location VARCHAR,
        artist_latitude VARCHAR,
        artist_longitude VARCHAR
    )
""")

time_table_create = ("""
    CREATE TABLE dim_time (
        start_time TIMESTAMP,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday SMALLINT
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    JSON {}
    REGION {}
    CREDENTIALS 'aws_iam_role={}';

""").format(log_data, log_jsonpath, region, aws_arn)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    REGION {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto';
""").format(song_data, region, aws_arn)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO fct_songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        MD5(se.userId::VARCHAR||se.sessionId::VARCHAR||se.ts::VARCHAR) AS songplay_id
        , TIMESTAMP 'epoch' + (se.ts/1000 * INTERVAL '1 second') AS start_time
        , se.userId AS user_id
        , se.level
        , ss.song_id
        , ss.artist_id
        , se.sessionId AS session_id
        , se.location
        , se.userAgent AS user_agent
    FROM staging_events AS se
    LEFT JOIN staging_songs AS ss
        ON se.artist = ss.artist_name
        AND se.song = ss.title
    WHERE page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
    SELECT
        DISTINCT userID AS user_id
        , firstName AS first_name
        , lastName AS last_name
        , gender
        , level
    FROM staging_events
    WHERE userID IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT song_id AS song_id
        , title
        , artist_id
        , year
        , duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO dim_artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT
        DISTINCT artist_id AS artist_id
        , artist_name
        , artist_location
        , artist_latitude
        , artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
    WITH timestamp_convert AS (
        SELECT
        DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time
        FROM staging_events
    )
    SELECT 
        start_time
        , EXTRACT(HOUR FROM start_time) AS hour
        , EXTRACT(DAY FROM start_time) AS day
        , EXTRACT(WEEK FROM start_time) AS week
        , EXTRACT(MONTH FROM start_time)  AS month
        , EXTRACT(YEAR FROM start_time) AS year
        , EXTRACT(DOW FROM start_time) AS weekday   
    FROM timestamp_convert
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
