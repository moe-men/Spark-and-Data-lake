# **Project: Spark and Data Lake for Sparkify**

## **introduction**
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## **Project Description**
- The task is building an ETL pipeline that :
    - extracts their data from S3 
    - processes them using Spark 
    - and loads the data back into S3 as a set of dimensional tables. 
This will allow their analytics team to continue finding insights in what songs their users are listening to.

## **Data**
- we will be working with two datasets that reside in S3. Here are the S3 links for each:
        - Song data: s3://udacity-dend/song_data
        - Log data: s3://udacity-dend/log_data


## **Database Schema**

- The database schema is a star schema that includes the following tables.
    - Fact Table :
            songplays - records in event data associated with song plays i.e. records with page NextSong
                column_names: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    - Dimension Tables
            users - users in the app
                column_names: user_id, first_name, last_name, gender, level
            songs - songs in music database
                column_names: song_id, title, artist_id, year, duration
            artists - artists in music database
                column_names: artist_id, name, location, lattitude, longitude
            time - timestamps of records in songplays broken down into specific units
                column_names: start_time, hour, day, week, month, year, weekday




## **How to run the Python scripts**
- The script reads song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3.

- To run the python scripts:
    - open the terminal
    - type **python etl.py**
    - hit enter
