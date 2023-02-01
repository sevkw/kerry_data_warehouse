import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
region = 'us-west-2'
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
        artist VARCHAR(128) NOT NULL,
        auth VARCHAR(10) NOT NULL,
        firstName VARCHAR(25) NOT NULL,
        gender VARCHAR(5) NOT NULL,
        itemInSession INTEGER NOT NULL,
        lastName VARCHAR(25) NOT NULL,
        length NUMERIC(10, 5),
        level VARCHAR(10) NOT NULL,
        location VARCHAR(50) NOT NULL,
        method VARCHAR(5) NOT NULL,
        page VARCHAR(10) NOT NULL,
        registration NUMERIC(20, 10) NOT NULL,
        sessionId INTEGER NOT NULL,
        song VARCHAR(128) NOT NULL,
        status INTEGER NOT NULL,
        ts BIGINT NOT NULL,
        userAgent VARCHAR(256) NOT NULL,
        userId INTEGER NOT NULL
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INTEGER NOT NULL,
        artist_id BIGINT NOT NULL,
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_name VARCHAR(50) NOT NULL,
        song_id VARCHAR(50) NOT NULL,
        title VARCHAR(100) NOT NULL,
        duration NUMERIC(10, 5) NOT NULL,
        year SMALLINT NOT NULL
    )
""")

songplay_table_create = ("""
    CREATE TABLE fct_songplays (
        songplay_id INTEGER NOT NULL PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR(10) NOT NULL,
        song_id VARCHAR(50) NOT NULL,
        artist_id BIGINT NOT NULL,
        session_id INTEGER NOT NULL,
        location VARCHAR(50) NOT NULL,
        user_agent VARCHAR(256) NOT NULL
    )
""")

user_table_create = ("""
    CREATE TABLE dim_users (
        user_id INTEGER NOT NULL PRIMARY KEY,
        first_name VARCHAR(25) NOT NULL,
        last_name VARCHAR(25) NOT NULL,
        gender VARCHAR(5) NOT NULL,
        level VARCHAR(10) NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE dim_songs (
        song_id VARCHAR(50) NOT NULL PRIMARY KEY,
        title VARCHAR(128) NOT NULL,
        artist_id BIGINT NOT NULL,
        year INTEGER NOT NULL,
        duration NUMERIC(10, 5) NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE dim_artists (
        artist_id BIGINT NOT NULL PRIMARY KEY,
        artist_name VARCHAR(128) NOT NULL,
        artist_location VARCHAR(50) NOT NULL,
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION
    )
""")

time_table_create = ("""
    CREATE TABLE dim_time (
        start_time TIMESTAMP NOT NULL,
        hour SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        year SMALLINT NOT NULL,
        weekday SMALLINT NOT NULL
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
        MD5(CONCAT(CAST(se.userId AS VARCHAR), CAST(se.sessionId AS VARCHAR), CAST(se.ts AS VARCHAR))) AS songplay_id
        , TO_TIMESTAMP(se.ts / 1000) AT TIME ZONE 'UTC' AS start_time
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
    SELECT 
        DISTINCT TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'UTC' AS start_time
        , EXTRACT(HOUR FROM TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'UTC') AS hour
        , EXTRACT(DAY FROM TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'UTC') AS day
        , EXTRACT(WEEK FROM TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'UTC') AS week
        , EXTRACT(MONTH FROM TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'UTC') AS month
        , EXTRACT(YEAR FROM TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'UTC') AS year
        , EXTRACT(DOW FROM TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'UTC') AS weekday   
    FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
