# Data Modeling with Postgres

## About The Project

This project creates a Data Warehouse in Redshift for the Sparkify mock music streaming app optimized for data analytics. This database consists of song and song play data in the schema below.

## Database Schema

The source data comes from log JSON files in the S3 bucket of the application. These files are copied into staging tables in redshift and after a insert is done from the staging into the final tables of the Data Warehouse. 

The final schema was design as a star schema to optimize queries of songplay data and song data as follows:

!["sparkifydb_erd"](images/sparkifydb_erd.png)

### Fact Table
`songplays`: long data from songs played in NextSong.
 - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- **PRIMARY KEY**: songplay_id.
- **DISTRIBUTION KEY**: user_id -> this fact table should be large and potentially will be joined by by the user_id to take user insights.
- **SORTING KEY**: start_time -> sorting the table by date to optimized analytics into periods of data.

### Dimension Tables
`users`: users data.
- user_id, first_name, last_name, gender, level
- **PRIMARY KEY**: songplay_id.
- **DISTRIBUTION KEY**: songplay_id -> this fact table should be large and potentially will be joined by all the dimensions tables.
- **SORTING KEY**: start_time -> sorting the table by date to optimized analytics into periods of data.

`songs`: songs data.
- song_id, title, artist_id, year, duration
- **PRIMARY KEY**: song_id.
- **DISTRIBUTION KEY**: song_id -> distributed by the main join parameter.
- **SORTING KEY**: year -> sorting the table by year to optimized analytics into periods of data.

`artists`: artists data.
- artist_id, name, location, latitude, longitude
- **PRIMARY KEY**: artist_id.
- **DISTRIBUTION KEY**: artist_id -> distributed by the main join parameter.
- **SORTING KEY**: name -> sorting the table by artist name to optimize ordered queries.

`time`: timestamp auxiliary table. 
- start_time, hour, day, week, month, year, weekday
- **PRIMARY KEY**: start_time.
- **DISTRIBUTION KEY**: diststyle all -> since this table should be small and is used by many tables in the schema, it is plausible to distributed in all style.
- **SORTING KEY**: start_time -> sorting the table by start_time to optimize analytics into periods of data.

## Getting Started

To run the project it is required a AWS user with privileges to create a redshift cluster and IAM role. Also, the scripts to create and insert data in the tables are in python.

### Prerequisites

* AWS account
* Python

### Configuration File

The user should create a `dwh.cfg` file with the fields as exemplified below:

```
[CLUSTER]
HOST=your_redshift_cluster_endpoint
DB_NAME=your_redshift_db_name
DB_USER=your_redshift_user
DB_PASSWORD=your_redshift_password
DB_PORT=your_redshift_port

[IAM_ROLE]
ARN=you_arn_role

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```

## Usage

The user can execute the `create_tables.py` to drop all previous project tables and create them. Later, the user can execute `etl.py` to insert the data into the staging tables and final tables. It is important to create the tables before running the etl.py as follows:

 ```sh
 python -m create_tables
 ```
 ```sh
 python -m etl
 ```

### Query Examples

1 - Selecting all the songs of Red Hot Chili Peppers with the song and artist id:

```sql
SELECT s.title
    , s.song_id
    , a.artist_id
    , a.name as artist
    , s.year
    , s.duration as song_length
FROM songs as s
JOIN artists a ON s.artist_id = a.artist_id
WHERE lower(a.name) = 'red hot chili peppers';
 ```

2 - Counting the number of unique users that played songs from a given artist_id:

```sql
SELECT t.year, 
        count(distinct s.user_id) as nunique_users
FROM songplay as s
JOIN time as t ON s.start_time = t.start_time
WHERE s.artist_id = 'AR5E44Z1187B9A1D74'
GROUP BY t.year;
 ```

### Optional

If you desire, there is a class with auxiliary methods to create and delete the cluster and IAM role for the Data Warehouse`ConfigureRedshift` in the `create_cluster.py` file. Before initiating the class, make sure to create a configuration file as shown below:

```
[AWS]
KEY=AKIA5373F364GG73C3ER
SECRET=PUWZI06gR2OE6xpreiUSyHiK1A91Xk9AvEPh/H7Q

[DWH] 
DWH_CLUSTER_TYPE=your_desired_cluster_type
DWH_NUM_NODES=your_desired_number_of_nodes
DWH_NODE_TYPE=your_desired_node_type
DWH_IAM_ROLE_NAME=your_desired_iam_role_name
DWH_CLUSTER_IDENTIFIER=your_desired_cluster_name
DWH_DB=your_desired_database_name
DWH_DB_USER=your_desired_username
DWH_DB_PASSWORD=your_desired_password
DWH_PORT=your_desired_redshift_port
```

Its usage is exemplified in the `Redshift DWH - Example.ipynb`.

## Files Description

- `sql_queries.py`: contains all the queries templates necessary to drop, create copy and insert in the project tables.

- `create_tables.py`: drops all pre existing tables for the project and creates the necessary tables.

- `etl.py`: execute the copy statement to the staging tables and the insert statement into the final tables.

- `create_cluster.py`: auxiliary class to create a Redshift cluster and role for the project.

- `Redshift DWH - Example.ipynb`: Jupyter Notebook to demonstrate how to create the data warehouse and query the tables in it.