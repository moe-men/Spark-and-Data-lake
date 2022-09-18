import configparser
from datetime import datetime
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    ingest input song_data from S3, processes that data using Spark to create table_songs and table_artists, and writes them as parquet to S3
    input : 
        - spark : spark session
        - input_data : path for initial json song_data
        - output_data : path to store songs_table and artists_table in parquet format
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # create a view
    df.createOrReplaceTempView("songs_data")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT DISTINCT 
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    FROM songs_data 
    WHERE song_id IS NOT NULL""")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'table_songs/')

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT DISTINCT 
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM songs_data WHERE artist_id IS NOT NULL""")  
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'table_artists/')


def process_log_data(spark, input_data, output_data):
    """
        ingest input log data from S3, processes that data using Spark to create table_users and table_times and table_songplays, and writes them as parquet to S3
    input : 
        - spark : spark session
        - input_data : path for initial json song_data
        - output_data : path to store songs_table and artists_table in parquet format
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    # create temp view
    df.createOrReplaceTempView("logs_data")
    
    # extract columns for users table    
    users_table = spark.sql("""
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM 
    logs_data 
    WHERE userId IS NOT NULL""")
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'table_users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn('datetime', get_datetime(df['ts'])) 
    
    # create temp view
    df.createOrReplaceTempView("time_data")
    
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT DISTINCT 
        datetime as start_time,
        hour(datetime) AS hour,
        day(datetime) AS day,
        weekofyear(datetime) AS week,
        month(datetime) AS month,
        year(datetime) AS year,
        dayofweek(datetime) AS weekday
    FROM time_data""") 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'table_times/')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # create temp view
    song_df.createOrReplaceTempView("songs_data")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT 
        monotonically_increasing_id() AS songplay_id,
        to_timestamp(logs_data.ts/1000) AS start_time,
        month(to_timestamp(logs_data.ts/1000)) AS month,
        year(to_timestamp(logs_data.ts/1000)) AS year,
        logs_data.userId AS user_id,
        logs_data.level AS level,
        songs_data.song_id AS song_id,
        songs_data.artist_id AS artist_id,
        logs_data.sessionId AS session_id,
        logs_data.location AS location,
        logs_data.userAgent AS user_agent
    FROM 
        logs_data JOIN songs_data ON logs_data.artist = songs_data.artist_name""")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'table_songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
