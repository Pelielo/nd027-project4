import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS", "ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS", "SECRET_ACCESS_KEY")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data_path, output_data_path):
    """Processes song data in .json format reading from 
    input_data_path using the spark session and outputs 
    artists.parquet and song.parquet to output_data_path"""

    # get path to song data files
    song_data_path = 'song_data/*/*/*/*.json'

    # define json schema to read song files
    
    songSchema = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType())
    ])

    # read song data file
    df = spark.read.json(input_data_path + song_data_path, schema=songSchema)
    df.show(5)

    # extract columns to create songs table
    songs_table = df.selectExpr('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    songs_table.show(5)
    songs_table.printSchema()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data_path + 'songs.parquet')

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude').dropDuplicates()
    artists_table.show(5)
    artists_table.printSchema()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data_path + 'artists.parquet')


def process_log_data(spark, input_data_path, output_data_path):
    """Processes log data in .json format reading from 
    input_data_path using the spark session and outputs 
    users.parquet, time.parquet and songplays.parquet 
    to output_data_path"""

    # get path to log data files
    log_data_path = "log_data/*.json"
    
    # get path to song data files
    song_data_path = 'song_data/*/*/*/*.json'

    # define json schema to read log files
    logSchema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", IntegerType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", IntegerType()),
        StructField("song", StringType()),
        StructField("status", IntegerType()),
        StructField("ts", StringType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType())
    ])

    # read log data file
    df = spark.read.json(input_data_path + log_data_path, schema=logSchema)
    df.show(5)
    df.printSchema()
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr('cast(userId as int) as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level').dropDuplicates()
    users_table.show(5)
    users_table.printSchema()
    
    # write users table to parquet files
    users_table.write.parquet(output_data_path + 'users.parquet')

    # extract columns to create time table
    df.createOrReplaceTempView('log_table')
    time_table = spark.sql("""
        select 
            a.ts as start_time,
            hour(a.ts) as hour,
            dayofmonth(a.ts) as day,
            weekofyear(a.ts) as week,
            month(a.ts) as month,
            year(a.ts) as year,
            dayofweek(a.ts) as weekday
        from (select cast(ts/1000 as timestamp) as ts from log_table) a
    """).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data_path + 'time.parquet')
    time_table.show(5)
    time_table.printSchema()

    # read in song data to use for songplays table
    # read song data file
    song_df = spark.read.json(input_data_path + song_data_path)
    song_df.show(5)
    song_df.printSchema()

    # join song dataset and log dataset
    joined_df = song_df.join(df, [(song_df.artist_name == df.artist) & (song_df.title == df.song) & (song_df.duration == df.length)], how='inner')
    joined_df.show(5)
    joined_df.printSchema()

    # extract columns from joined song and log datasets to create songplays table 
    joined_df.createOrReplaceTempView('joined_table')
    songplays_table = spark.sql("""
        select
            cast(ts/1000 as timestamp) as start_time,
            year(cast(ts/1000 as timestamp)) as year,
            month(cast(ts/1000 as timestamp)) as month,
            cast(userId as int) as user_id,
            level,
            song_id,
            artist_id,
            sessionId as session_id,
            location,
            userAgent as user_agent
        from joined_table  
    """).dropDuplicates()
    songplays_table.show(5)
    songplays_table.printSchema()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data_path + 'songplays.parquet')


def main():
    spark = create_spark_session()

    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", os.environ["AWS_ACCESS_KEY_ID"])
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", os.environ["AWS_SECRET_ACCESS_KEY"])

    input_data_path = "s3a://dend-bucket-2a95/"
    # input_data_path = "data/"
    output_data_path = "s3a://dend-bucket-2a95/output_data_project4/"
    # output_data_path = "output_data/"

    
    process_song_data(spark, input_data_path, output_data_path)    
    process_log_data(spark, input_data_path, output_data_path)


if __name__ == "__main__":
    main()
