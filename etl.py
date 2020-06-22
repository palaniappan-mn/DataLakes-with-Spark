import configparser
from datetime import datetime
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWSCREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWSCREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """This method creates a spark session to establish connection with the 
    driver program through which the client and the namenode can intereact.
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """This method reads the songs data from the json files situated in the
    S3 data lake, creates the songs table and artists table using spark and
    writes it back to S3.

    Input: 
    spark: This is the spark session object
    input_data: The S3 location of the input data
    output_data: The S3 location to which the output data should be written 
    """

    # get filepath to song data file
    song_data = input_data + 'song_data/[A-Z]/[A-Z]/[A-Z]/*\.json'
    
    # read song data file
    df = spark. \
        read. \
        format('json'). \
        load(song_data)

    # extract columns to create songs table
    songs_table = df. \
        select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    songs_table_path = output_data + '/songs_table'
    
    # write songs table to parquet files partitioned by year and artist
    songs_table. \
        write. \
        format('parquet'). \
        partitionBy('year','artist_id'). \
        mode('overwrite'). \
        save(songs_table_path)

    # extract columns to create artists table
    artists_table = df. \
        select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') .\
        dropDuplicates()

    artists_table_path = output_data + '/artists_table'

    # write artists table to parquet files
    artists_table. \
        write. \
        format('parquet'). \
        mode('overwrite'). \
        save(artists_table_path)


def process_log_data(spark, input_data, output_data):
    """This method reads the event log data from the json files situated in the
    S3 data lake, creates the users table, time table and songplays table using 
    spark and writes it back to S3.

    Input: 
    spark: This is the spark session object
    input_data: The S3 location of the input data
    output_data: The S3 location to which the output data should be written
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/*\.json'

    # read log data file
    df = spark. \
        read. \
        format('json'). \
        load(log_data)
    
    # filter by actions for song plays
    df = df. \
        filter('page == "NextSong"')

    # extract columns for users table    
    users_table = df. \
        select('userId', 'firstName', 'lastName', 'gender', 'level'). \
        dropDuplicates()
    
    users_table_path = output_data + "/users_table"

    # write users table to parquet files
    users_table. \
        write. \
        format('parquet'). \
        mode('overwrite'). \
        load(users_table_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df. \
        select(date_format('timestamp','yyyy-MM-dd HH:mm:ss').alias('Start_time'),
               hour('timestamp').alias('Hour'),
               dayofmonth('timestamp').alias('Day'),
               weekofyear('timestamp').alias('Week'),
               month('timestamp').alias('Month'),
               year('timestamp').alias('Year'),
               dayofweek('timestamp').alias('Weekday')). \
        dropDuplicates()
    
    time_table_path = output_data + '/time_table'

    # write time table to parquet files partitioned by year and month
    time_table. \
        write. \
        partitionBy('Year','Month')
        format('parquet'). \
        mode('overwrite'). \
        save(time_table_path)

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/[A-Z]/[A-Z]/[A-Z]/*\.json'

    song_df = spark. \
        read. \
        format('json'). \
        load(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df. \
        join('df',
            song_df.artist_name==df.artist and
            song_df.duration==df.length and
            song_df.title==df.song,
            'right')

    songplays_table = songplays_table. \
        select(date_format('timestamp','yyyy-MM-dd HH:mm:ss').alias('Start_time'),
               'userId',
               'level',
               'song_id',
               'artist_id',
               'sessionId',
               'artist_location',
               'userAgent')

    songplays_table = songplays_table. \
        withColumn('songplay_id',monotonically_increasing_id()). \
        withColumn('year',year('Start_time')). \
        withColumn('month',month('Start_time'))

    songplays_table_path = output_data '/songplays_table'

    # write songplays table to parquet files partitioned by year and month
    songplays_table. \
        write. \
        format('parquet'). \
        partitionBy('year','month'). \
        load(songplays_table_path)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
