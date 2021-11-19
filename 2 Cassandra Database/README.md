# Data Modeling with Postgres

## About The Project

This project creates the database in Apache Cassandra for the Sparkify fictitious music streaming app. The Analytics team wants to explore user and music preferences by answering specific business questions. The data is modelled around the business necessity to optimize these possible queries.

## Source Data

The source data is the [Million Song Dataset](http://millionsongdataset.com/) and a sample of these dataset is processed into a single csv in the example below.

!["dataset"](images/image_event_datafile_new.jpg)

## Database Keyspace

We want to answer the followind business questions:

#### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
`SELECT artist_name, song_title, song_length FROM sessions WHERE session_id = 338 AND item_in_session = 4`

#### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
`SELECT artist_name, song_title (sorted by item_in_session), user_first_name, user_last_name FROM user_library WHERE user_id = 10 AND session_id = 182`

#### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
`SELECT user_first_name, user_last_name FROM music_library WHERE song_title = 'All Hands Against His Own'`

### Tables

To answer these questions, the tables below were created:

`sessions`: optimized to query session data.
- PRIMARY KEY: session_id, item_in_session
- PARTITIONING KEY: session_id  -> it will always be in the where and hopefully can distribute the data evenly.
- CLUSTERING KEY: item_in_session -> it will also be in the where clause and makes the PK unique.

`user_library`: optimized to query users. The user are sorted by the item_in_session.
- PRIMARY KEY: (user_id, session_id), item_in_session
- PARTITIONING KEY: user_id, session_id  -> since the user_id is a 1:N relationship with session_id and both columns will be in the where clause, it is reasonable to use them as a PK, given that the addition of the user_id will not mess with the data distribution through the nodes.
- CLUSTERING KEY: item_in_session -> it will sort the songs and make the PK unique.

`music_library`: optimized to user information giving a song.
- PRIMARY KEY: song_title, user_id -> the analysis here is just to retrieve unique users for a giving song, the PK needs to be unique only for these two columns
- PARTITIONING KEY: song_title  -> since the song_title will always be in the where clause.
- CLUSTERING KEY: user_id -> to make users unique for each song.

## Getting Started

To run the project just follow the notebook in the repository `Sparkfy Cassandra Database.ipynb`. 

### Prerequisites

* Python
* Jupyter Notebook
* [Million Song Dataset](http://millionsongdataset.com/)
* Apache Cassandra