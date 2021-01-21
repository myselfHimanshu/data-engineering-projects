import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE staging_events IF EXISTS"
staging_songs_table_drop = "DROP TABLE staging_songs IF EXISTS"
songplay_table_drop = "DROP TABLE songplays IF EXISTS"
user_table_drop = "DROP TABLE users IF EXISTS"
song_table_drop = "DROP TABLE songs IF EXISTS"
artist_table_drop = "DROP TABLE artists IF EXISTS"
time_table_drop = "DROP TABLE time_table IF EXISTS"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          CHAR(1),
        itemInSession   INTEGER,
        lastName        VARCHAR,
        length          FLOAT,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    FLOAT,
        sessionId       BIGINT,
        song            VARCHAR,
        status          INTEGER,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     DECIMAL(9,6),
        artist_longitude    DECIMAL(9,6),
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER CHECK (yeat>=0)
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplayid      IDENTITY(0,1) PRIMARY KEY,
        start_time      TIMESTAMP sortkey,
        user_id         VARCHAR NOT NULL,
        level           VARCHAR NOT NULL,
        song_id         VARCHAR,
        artist_id       VARCHAR,
        session_id      BIGINT NOT NULL,
        location        VARCHAR,
        user_agent      VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     VARCHAR PRIMARY KEY,
        first_name  VARCHAR,
        last_name   VARCHAR,
        gender      CHAR(1),
        level       VARCHAR NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INT CHECK (year >= 0),
        duration FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude DECIMAL(9,6),
        longitude DECIMAL(9,6)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT CHECK (hour >= 0),
        day INT CHECK (day >= 0),
        week INT CHECK (week >= 0),
        month INT CHECK (month >= 0),
        year INT CHECK (year >= 0),
        weekday INT CHECK (weekday >= 0)
    );
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

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
