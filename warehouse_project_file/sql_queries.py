"""
In this python module, we define sql queries for creating and populating tables.
"""

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
        CREATE TABLE staging_events
        (
            event_id INT IDENTITY(0,1),
            artist_name VARCHAR(255),
            auth VARCHAR(50),
            user_first_name VARCHAR(255),
            user_gender  VARCHAR(1),
            item_in_session	INTEGER,
            user_last_name VARCHAR(255),
            song_length	DOUBLE PRECISION, 
            user_level VARCHAR(50),
            location VARCHAR(255),	
            method VARCHAR(25),
            page VARCHAR(35),	
            registration VARCHAR(50),	
            session_id	BIGINT,
            song_title VARCHAR(255),
            status INTEGER, 
            ts VARCHAR(50),
            user_agent TEXT,	
            user_id VARCHAR(100),
            PRIMARY KEY (event_id)
        );
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
    song_id VARCHAR(100),
    num_songs INTEGER,
    artist_id VARCHAR(100),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INTEGER,
    PRIMARY KEY (song_id)
    );
""")

songplay_table_create = ("""
CREATE TABLE songplays
(
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP,
    user_id VARCHAR(50),
    level VARCHAR(50),
    song_id VARCHAR(100),
    artist_id VARCHAR(100),
    session_id BIGINT,
    location VARCHAR(255),
    user_agent TEXT,
    PRIMARY KEY (songplay_id)
    );
""")

user_table_create = ("""
CREATE TABLE users
(
    user_id VARCHAR,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(50),
    PRIMARY KEY (user_id)
    );
""")

song_table_create = ("""
CREATE TABLE songs
(
    song_id VARCHAR(100),
    title VARCHAR(255),
    artist_id VARCHAR(100) NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id)
    );
""")

artist_table_create = ("""
CREATE TABLE artists
(
    artist_id VARCHAR(100),
    name VARCHAR(255),
    location VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id)
    );
""")

time_table_create = ("""
CREATE TABLE time
(
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time)
    );
""")

# STAGING TABLES

staging_events_copy = (F"""
                          COPY staging_events
                          FROM {config.get('S3','LOG_DATA')}
                          iam_role {config.get('IAM_ROLE', 'ARN')}
                          json {config.get('S3','LOG_JSONPATH')}  region { config['S3']['REGION']};
""").format()

staging_songs_copy = (f"""
                          COPY staging_songs
                          FROM {config.get('S3','SONG_DATA')}
                          iam_role {config.get('IAM_ROLE', 'ARN')}
                          FORMAT AS JSON 'auto' 
                          region { config['S3']['REGION']};
""").format()

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
        TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time, 
        se.user_id, 
        se.user_level, 
        ss.song_id,
        ss.artist_id, 
        se.session_id,
        se.location, 
        se.user_agent
FROM staging_events AS se, staging_songs AS ss
WHERE se.page = 'NextSong' 
AND se.song_title = ss.title 
AND se.artist_name = ss.artist_name 
AND se.song_length = ss.duration
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT  
        user_id, 
        user_first_name, 
        user_last_name, 
        user_gender, 
        user_level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration) 
        SELECT DISTINCT 
                    song_id, 
                    title,
                    artist_id,
                    year,
                    duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude) 
        SELECT DISTINCT 
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
SELECT start_time, 
    EXTRACT(hour from start_time),
    EXTRACT(day from start_time),
    EXTRACT(week from start_time), 
    EXTRACT(month from start_time),
    EXTRACT(year from start_time), 
    EXTRACT(dayofweek from start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
