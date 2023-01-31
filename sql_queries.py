import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
region = 'us-west-2'
aws_arn = config.get("IAM_ROLE", "ARN")

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
        sessionid INTEGER NOT NULL,
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
        artist_lattitude DOUBLE PRECISION,
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
        name VARCHAR(128) NOT NULL,
        location VARCHAR(50) NOT NULL,
        artist_lattitude DOUBLE PRECISION,
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
    FROM 's3://udacity-dend/log_data'
    JSON 's3://udacity-dend/log_json_path.json'
    REGION {}
    CREDENTIALS 'aws_iam_role={}';

""").format(region, aws_arn)

staging_songs_copy = ("""
    COPY staging_songs
    FROM 's3://udacity-dend/song_data'
    REGION {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto';
""").format(region, aws_arn)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
