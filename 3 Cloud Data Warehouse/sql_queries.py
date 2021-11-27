"""
SQL support queries to drop, create and insert in the Redshift tables.
"""
import configparser

# CONFIG
# getting default configuration parameters for Redshift and S3

config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']

LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES
# drops tables before creating

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# creates tables before inserting

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
          artist        VARCHAR
        , auth          VARCHAR
        , firstName     VARCHAR
        , gender        VARCHAR
        , itemInSession BIGINT NOT NULL
        , lastName      VARCHAR
        , length        FLOAT
        , level         VARCHAR
        , location      VARCHAR
        , method        VARCHAR
        , page          VARCHAR
        , registration  BIGINT
        , sessionId     BIGINT
        , song          VARCHAR
        , status        INT
        , ts            TIMESTAMP
        , userAgent     VARCHAR
        , userId        BIGINT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
          song_id          VARCHAR
        , num_songs        INT
        , title            VARCHAR
        , artist_name      VARCHAR
        , artist_latitude  FLOAT
        , year             INT
        , duration         FLOAT
        , artist_id        VARCHAR
        , artist_longitude FLOAT
        , artist_location  VARCHAR
    );

""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id BIGINT IDENTITY(0,1)
      , start_time  TIMESTAMP NOT NULL
      , user_id     INT NOT NULL
      , level       VARCHAR
      , song_id     VARCHAR NOT NULL
      , artist_id   VARCHAR NOT NULL
      , session_id  BIGINT NOT NULL
      , location    VARCHAR
      , user_agent  VARCHAR
      , PRIMARY KEY (songplay_id)
      , FOREIGN KEY (user_id) REFERENCES users(user_id)
      , FOREIGN KEY (song_id) REFERENCES songs(song_id)
      , FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
      , FOREIGN KEY (start_time) REFERENCES time(start_time)
    )
    distkey(user_id)
    sortkey(start_time);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    BIGINT
      , first_name VARCHAR NOT NULL
      , last_name  VARCHAR NOT NULL
      , gender     VARCHAR
      , level      VARCHAR
      , PRIMARY KEY (user_id)
    )
    distkey(user_id);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id   VARCHAR
      , title     VARCHAR NOT NULL
      , artist_id VARCHAR NOT NULL
      , year      INT
      , duration  FLOAT
      , PRIMARY KEY (song_id)
      , FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    )
    distkey(song_id)
    sortkey(year);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR
      , name      VARCHAR NOT NULL
      , location  VARCHAR
      , latitude  INT
      , longitude FLOAT
      , PRIMARY KEY (artist_id)
    )
    distkey(artist_id)
    sortkey(name);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP
      , hour       INT NOT NULL
      , day        INT NOT NULL
      , week       INT NOT NULL
      , month      INT NOT NULL
      , year       INT NOT NULL
      , weekday    INT NOT NULL
      , PRIMARY KEY (start_time)
    )
    diststyle all
    sortkey(start_time);
""")

# STAGING TABLES
# staging tables as intermediate step before the final tables
# the data is copied to optmize the staging

# using json format in S3 to copy
# timeformat to epoch to avoid errors
# computedate and statupdate off to make the copy faster for staging
staging_events_copy = (f"""
    COPY staging_events
    FROM {LOG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ROLE}'
    REGION 'us-west-2'
    JSON {LOG_JSONPATH}
    TIMEFORMAT 'epochmillisecs'
    EMPTYASNULL
    TRIMBLANKS
    BLANKSASNULL
    COMPUPDATE OFF
    STATUPDATE OFF;
""")

# auto json format to guarantee correct insert order
# computedate and statupdate off to make the copy faster for staging
staging_songs_copy = (f"""
    COPY staging_songs
    FROM {SONG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ROLE}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto'
    COMPUPDATE OFF
    EMPTYASNULL
    BLANKSASNULL
    TRIMBLANKS
    STATUPDATE OFF;
""")

# FINAL TABLES
# inserting in the final tables from staging

# JOIN stages by artist name, song title and song duration to get all fields
# different insert for identity table.
songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id,
                          session_id, location, user_agent)
    SELECT DISTINCT
           e.ts        as start_time
         , e.userId    as user_id
         , e.level     as level
         , s.song_id   as song_id
         , s.artist_id as artist_id
         , e.sessionId as session_id
         , e.location  as location
         , e.userAgent as user_agent
    FROM staging_events e
    JOIN staging_songs s ON (
        lower(e.artist) = lower(s.artist_name)
        AND lower(e.song) = lower(s.title)
        AND e.length = s.duration
    );
""")

user_table_insert = ("""
    INSERT INTO users (
        SELECT DISTINCT 
               e.userId    as user_id
             , e.firstName as first_name
             , e.lastName  as last_name
             , e.gender    as gender
             , e.level     as level
        FROM staging_events e
        WHERE e.userId IS NOT NULL
    );
""")

song_table_insert = ("""
    INSERT INTO songs (
        SELECT DISTINCT
               s.song_id   as song_id
             , s.title     as title
             , s.artist_id as artist_id
             , s.year      as year
             , s.duration  as duration
        FROM staging_songs s
        WHERE s.song_id IS NOT NULL
    );
""")

artist_table_insert = ("""
    INSERT INTO artists (
        SELECT DISTINCT
               s.artist_id        as artist_id
             , s.artist_name      as name
             , s.artist_location  as location
             , s.artist_latitude  as latitude
             , s.artist_longitude as longitude
        FROM staging_songs s
        WHERE s.artist_id IS NOT NULL
    );
""")

time_table_insert = ("""
    INSERT INTO time (
        SELECT DISTINCT
               e.ts                     as start_time
             , EXTRACT(hour FROM ts)    as hour
             , EXTRACT(day FROM ts)     as day
             , EXTRACT(week FROM ts)    as week
             , EXTRACT(month FROM ts)   as month
             , EXTRACT(year FROM ts)    as year
             , EXTRACT(weekday FROM ts) as weekday
        FROM staging_events e
        WHERE e.ts IS NOT NULL
    );
""")

# QUERY LISTS
# list of queries to create_tables.py and etl.py

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create, user_table_create,
                        artist_table_create, song_table_create,
                        time_table_create, songplay_table_create
                        ]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert,
                        artist_table_insert, song_table_insert,
                        time_table_insert]
