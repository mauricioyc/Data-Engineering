"""
SQL support queries to drop, create and insert in the tables for the sparkifydb
"""

# DROP TABLES
# queries to drop tables in the sparkifydb schema

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# queries to create tables in the sparkifydb schema

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id bigserial,
                                start_time timestamp NOT NULL,
                                user_id int NOT NULL,
                                level varchar,
                                song_id varchar,
                                artist_id varchar,
                                session_id bigint,
                                location varchar,
                                user_agent varchar,
                                PRIMARY KEY (songplay_id),
                                FOREIGN KEY (user_id)
                                    REFERENCES users(user_id),
                                FOREIGN KEY (song_id)
                                    REFERENCES songs(song_id),
                                FOREIGN KEY (artist_id)
                                    REFERENCES artists(artist_id),
                                FOREIGN KEY (start_time)
                                    REFERENCES time(start_time)
                            );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                            user_id int,
                            first_name varchar,
                            last_name varchar,
                            gender varchar,
                            level varchar,
                            PRIMARY KEY (user_id)
                        );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                            song_id varchar,
                            title varchar,
                            artist_id varchar,
                            year int,
                            duration float,
                            PRIMARY KEY (song_id)
                        );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                            artist_id varchar,
                            name varchar,
                            location varchar,
                            latitude float,
                            longitude float,
                            PRIMARY KEY (artist_id)
                        );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                            start_time timestamp,
                            hour int,
                            day int,
                            week int,
                            month int,
                            year  int,
                            weekday int,
                            PRIMARY KEY (start_time)
                        );
""")

# INSERT RECORDS
# generic queries to insert data into sparkifydb tables

songplay_table_insert = ("""INSERT INTO songplays (start_time,
                            user_id, level, song_id, artist_id, session_id,
                            location, user_agent)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name,
                        gender, level)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) DO
                            UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year,
                        duration)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location,
                          latitude, longitude)
                          VALUES (%s, %s, %s, %s, %s)
                          ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month,
                        year, weekday)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS
# select a song_id and artist_id from title, artist and duration

song_select = ("""SELECT s.song_id
                       , a.artist_id
                  FROM songs s
                  JOIN artists a ON a.artist_id = s.artist_id
                  WHERE s.title = %s AND
                        a.name = %s AND
                        s.duration = %s
""")

# QUERY LISTS
# list of queries to be used in the create_tables routine

create_table_queries = [user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create, songplay_table_create]

drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]
