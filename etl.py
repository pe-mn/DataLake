import configparser
from time import time
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, desc, substring
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create Spark Session with the hadoop-aws package
    which is used for us to connect with Amazon S3 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Args:
        spark: spark session
        input_data: Path to input data
        output_data: Path to output data
    Returns:
        Outputs the songs_table and the artists_table to S3
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json" 
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write\
    .partitionBy("year", "artist_id")\
    .mode('overwrite')\
    .parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    cols = ['artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    cols = [col + ' as ' + col[7:] for col in cols]
    artists_table = df.selectExpr('artist_id', *cols) 
    
    # write artists table to parquet files
    artists_table.write\
    .parquet(os.path.join(output_data, 'songs'),
             mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Args:
        spark: spark session
        input_data: Path to input data
        output_data: Path to output data
    Returns:
        Outputs the users_table, time_table and the songplays_table into S3
    """  
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json" # replace 2018/11/ with */*/ when submitting  

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(" page = 'NextSong'")

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", 
                                 "firstName as first_name",
                                 "lastName as last_name",
                                 "gender", 
                                 "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users")
                              , mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
#     # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 
    
    # extract columns to create time table
    time_table = df.select('start_time')
    # list of functions
    funcs = [F.hour, F.dayofmonth, F.weekofyear, F.month, F.year, F.dayofweek]
    cols = ['hour', 'day', 'week', 'month', 'year', 'weekday']
    time_cols = [(col, func('start_time'))for func, col in zip(funcs, cols)]
    for col in time_cols:
        time_table = time_table.withColumn(*col) 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite")\
    .parquet(os.path.join(output_data, "times"))

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data', '*', '*', '*'))

    # extract columns from joined song and log datasets to create songplays table 
    df = df.orderBy('ts')
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())

    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('events')

    # include year and month to allow parquet partitioning
    songplays_table = spark.sql("""
        SELECT
            e.songplay_id,
            e.start_time,
            e.userId as user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId as session_id,
            e.location,
            e.userAgent as user_agent,
            year(e.start_time) as year,
            month(e.start_time) as month
        FROM events e
        LEFT JOIN songs s ON
            e.song = s.title AND
            e.artist = s.artist_name
    """) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), 
                                  partitionBy=['year', 'month'],
                                  mode="overwrite")

    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./Results/"
#      input_data, output_data = './data/', './output/'  # Uncomment for local mode
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

