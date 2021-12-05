import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, dayofmonth, dayofweek, from_unixtime,
                                   hour, lower, monotonically_increasing_id,
                                   month, weekofyear, year)
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates and returns a Spark Session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Reads song data from the given S3 bucket.
    - Creates a auxiliary song_data table for Spark SQL.
    - Creates songs table from song_data.
    - Creates artists table from song_data.
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # read song data file with schema
    song_schema = StructType([
        StructField("num_songs",        IntegerType()),
        StructField("artist_id",        StringType()),
        StructField("artist_latitude",  DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location",  StringType()),
        StructField("artist_name",      StringType()),
        StructField("song_id",          StringType()),
        StructField("title",            StringType()),
        StructField("duration",         DoubleType()),
        StructField("year",             IntegerType()),
    ])
    df = spark.read.json(song_data, schema=song_schema)

    # create table for spark sql
    df.createOrReplaceTempView("song_data")

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT DISTINCT
               s.song_id   as song_id
             , s.title     as title
             , s.artist_id as artist_id
             , s.year      as year
             , s.duration  as duration
        FROM song_data s
        WHERE s.song_id IS NOT NULL
    """)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
               .partitionBy("year", "artist_id") \
               .mode("overwrite") \
               .parquet(os.path.join(output_data, "songs"))

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT
               s.artist_id        as artist_id
             , s.artist_name      as name
             , s.artist_location  as location
             , s.artist_latitude  as latitude
             , s.artist_longitude as longitude
        FROM song_data s
        WHERE s.artist_id IS NOT NULL
    """)

    # write artists table to parquet files
    artists_table.write \
                 .mode("overwrite") \
                 .parquet(os.path.join(output_data, "artists"))


def process_log_data(spark, input_data, output_data):
    """
    - Reads log data from the given S3 bucket.
    - Filter the NextSong pages and add a datetime column from epoch timestamp.
    - Creates a auxiliary log_data table for Spark SQL.
    - Creates users table from log_data.
    - Creates time table from log_data.
    - Creates songplays table from log_data.
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file with schema
    log_schema = StructType([
        StructField("artist",        StringType()),
        StructField("auth",          StringType()),
        StructField("firstName",     StringType()),
        StructField("gender",        StringType()),
        StructField("itemInSession", LongType()),
        StructField("lastName",      IntegerType()),
        StructField("length",        DoubleType()),
        StructField("level",         StringType()),
        StructField("location",      StringType()),
        StructField("method",        StringType()),
        StructField("page",          StringType()),
        StructField("registration",  DoubleType()),
        StructField("sessionId",     DoubleType()),
        StructField("song",          StringType()),
        StructField("status",        IntegerType()),
        StructField("ts",            TimestampType()),
        StructField("userAgent",     IntegerType()),
        StructField("userId",        LongType()),
    ])
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(col("page") == "NextSong")

    # create datetime column from original timestamp column
    df = df.withColumn("datetime", from_unixtime(col("ts")/1000))

    # create table for spark sql
    df.createOrReplaceTempView("log_data")

    # extract columns for users table
    users_table = spark.sql("""
        SELECT DISTINCT
               e.userId    as user_id
             , e.firstName as first_name
             , e.lastName  as last_name
             , e.gender    as gender
             , e.level     as level
        FROM log_data e
        WHERE e.userId IS NOT NULL
    """)

    # write users table to parquet files
    users_table.write \
               .mode("overwrite") \
               .parquet(os.path.join(output_data, "users"))

    # extract columns to create time table
    time_table = spark.sql("""
        SELECT DISTINCT
               e.datetime           as start_time
             , hour(datetime)       as hour
             , day(datetime)        as day
             , weekofyear(datetime) as week
             , dayofmonth(datetime) as month
             , year(datetime)       as year
             , CASE WHEN dayofweek(datetime) in (1,7)
                    THEN FALSE ELSE TRUE END as weekday
        FROM log_data e
        WHERE e.ts IS NOT NULL
    """)

    # write time table to parquet files partitioned by year and month
    time_table.write \
              .partitionBy("year", "month") \
              .mode("overwrite") \
              .parquet(os.path.join(output_data, "time"))

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # read song data file with schema
    song_schema = StructType([
        StructField("num_songs",        IntegerType()),
        StructField("artist_id",        StringType()),
        StructField("artist_latitude",  DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location",  StringType()),
        StructField("artist_name",      StringType()),
        StructField("song_id",          StringType()),
        StructField("title",            StringType()),
        StructField("duration",         DoubleType()),
        StructField("year",             IntegerType()),
    ])
    song_df = spark.read.json(song_data, schema=song_schema)

    # create table for spark sql
    song_df.createOrReplaceTempView("song_data")

    # extract columns from joined song and log datasets to create songplays \
    # table
    songplays_table = spark.sql("""
        SELECT DISTINCT
               ROW_NUMBER() OVER
                    (ORDER BY monotonically_increasing_id()) as songplay_id
             , e.datetime  as start_time
             , e.userId    as user_id
             , e.level     as level
             , s.song_id   as song_id
             , s.artist_id as artist_id
             , e.sessionId as session_id
             , e.location  as location
             , e.userAgent as user_agent
        FROM log_data e
        JOIN song_data s ON (
            lower(e.artist) = lower(s.artist_name)
            AND lower(e.song) = lower(s.title)
            AND e.length = s.duration
        )
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn("year", year("start_time")) \
                   .withColumn("month", month("start_time")) \
                   .write \
                   .partitionBy("year", "month") \
                   .mode("overwrite") \
                   .parquet(os.path.join(output_data, "songplays"))


def main():
    """
    - Creates a Spark Session.
    - Run song_data process, which creates Songs and Artists tables.
    - Runs log_data process, which creates Users, Time and Songplays tables.
    """

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://de-spark-teste/output_data/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
