import configparser
from datetime import datetime
import os
from pathlib import Path
import findspark
findspark.init("C:\Spark")
import random
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, \
     DoubleType as Dbl, LongType as Long, StringType as Str, \
     IntegerType as Int, DecimalType as Dec, DateType as Date, \
     TimestampType

# spark = SparkSession \
#         .builder \
#          \
#         .getOrCreate()
# df = spark.range(0, 10)

config = configparser.ConfigParser()
config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['altman_udacity_datalake']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['altman_udacity_datalake']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():

    spark = SparkSession \
        .builder \
        .appName("Datalake") \
        .getOrCreate()
    #    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    return spark


def process_song_data(spark, input_data_path, output_data_dir):
    """
    This function allows to process JSON files that describe songs data.
    Based on this data, we create 2 tables: songs and artists.
    Those tables are saved in the parquet format.

    Inputs:
        spark: spark session object
        input_data_path: str input path to the place where songs JSON can be found
        output_data_dir: str, path to the directory where we save table in the parquet format
    """
    # get filepath to song data file
    song_data_path = input_data_path + 'song_data/*/*/*/*.json'

    # read song data file
    original_song_df = spark.read.json(song_data_path)

    # extract columns to create songs table
    songs_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = original_song_df.select(songs_columns)
    songs_table = songs_table.dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    output_songs_table_path = output_data_dir + "/songs.parquet"
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(output_songs_table_path)

    # extract columns to create artists table
    artists_columns = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = original_song_df.select(artists_columns).withColumnRenamed("artist_name", "name") \
                                                            .withColumnRenamed("artist_location", "location") \
                                                            .withColumnRenamed("artist_latitude", "lattitude") \
                                                            .withColumnRenamed("artist_longitude", "longitude")
    artists_table = artists_table.dropDuplicates()

    # write artists table to parquet files
    output_artists_table_path = output_data_dir + "/artists.parquet"
    artists_table.write.mode("overwrite").parquet(output_artists_table_path)


def process_log_data(spark, input_data_path, output_data_dir):
    """
    This function allows to process JSON files that describe log data.
    Based on this data, we create the folllowing 3 tables: times, users and songplays.
    Those tables are saved in the parquet format.

    Inputs:
        spark: spark session object
        input_data_path: str input path to the place where songs JSON can be found
        output_data_dir: str, path to the directory where we save table in the parquet format
    """
    # get filepath to log data file
    log_data_path = input_data_path + 'log_data/*.json'

    # read log data file
    log_data_df = spark.read.json(log_data_path)

    # filter by actions for song plays
    log_data_df = log_data_df.filter(log_data_df.page == 'NextSong')

    # extract columns for users table
    users_columns = ["userId", "firstName", "lastName", "gender", "level"]
    users_table = log_data_df.select(users_columns).withColumnRenamed("userId", "user_id") \
                                                    .withColumnRenamed("firstName", "first_name") \
                                                    .withColumnRenamed("lastName", "last_name")
    users_table = users_table.dropDuplicates()

    # write users table to parquet files
    output_users_table_path = output_data_dir + "/users.parquet"
    users_table.write.mode("overwrite").parquet(output_users_table_path)

#
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000)), TimestampType())
    log_data_df = log_data_df.withColumn('timestamp', get_timestamp(log_data_df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    log_data_df = log_data_df.withColumn("datetime", get_datetime(log_data_df.ts))
    #
    # # extract columns to create time table
    # # start_time, hour, day, week, month, year, weekday
    time_table = log_data_df.selectExpr("timestamp AS start_time",
                                        "hour(timestamp) AS hour",
                                        "dayofmonth(timestamp) AS day",
                                        "weekofyear(timestamp) AS week",
                                        "month(timestamp) AS month",
                                        "year(timestamp) AS year",
                                        "dayofweek(timestamp) AS weekday"
                                        )
    time_table = time_table.dropDuplicates()


    # write time table to parquet files partitioned by year and month
    output_time_table_path = output_data_dir + "/time.parquet"
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_time_table_path)


#     # read in song data to use for songplays table
    song_df = spark.read.json(input_data_path + 'song_data/*/*/*/*.json')

    song_df.createOrReplaceTempView("song_table")
    log_data_df.createOrReplaceTempView("log_table")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
            SELECT
                monotonically_increasing_id() AS songplay_id,
                log_table.timestamp AS start_time ,
                log_table.userId AS user_id,
                log_table.level AS level,
                song_table.song_id AS song_id,
                song_table.artist_id AS artist_id,
                log_table.sessionId AS session_id,
                log_table.location AS location,
                log_table.userAgent AS user_agent,
                year(log_table.timestamp) AS year,
                month(log_table.timestamp) AS month
            FROM song_table
            JOIN log_table ON (log_table.song = song_table.title AND log_table.artist = song_table.artist_name
            AND log_table.length = song_table.duration)
    """)

    # write songplays table to parquet files partitioned by year and month

    output_time_table_path = output_data_dir + "/songplays.parquet"
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_time_table_path)
    print(songplays_table.show(5))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    input_data = "data/"
    output_data = "outputs"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
