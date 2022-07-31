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
import pyspark

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
    # get filepath to song data file
    song_data_path = input_data_path + 'song_data/A/A/A/*.json'

    # read song data file
    original_song_df = spark.read.json(song_data_path)
    print(original_song_df.show(5))
    # ['artist_id', 'artist_latitude', 'artist_location', 'artist_longitude', 'artist_name', 'duration', 'num_songs', 'song_id', 'title', 'year']
    print(original_song_df.columns)

    # extract columns to create songs table
    songs_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = original_song_df.select(songs_columns)
    songs_table = songs_table.dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    output_songs_table_path = output_data_dir + "/songs.parquet"
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(output_songs_table_path)
    #
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
    print(artists_table.columns)

    print("end process_song_data")


# def process_log_data(spark, input_data, output_data):
#     # get filepath to log data file
#     log_data =
#
#     # read log data file
#     df =
#
#     # filter by actions for song plays
#     df =
#
#     # extract columns for users table
#     artists_table =
#
#     # write users table to parquet files
#     artists_table
#
#     # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df =
#
#     # create datetime column from original timestamp column
#     get_datetime = udf()
#     df =
#
#     # extract columns to create time table
#     time_table =
#
#     # write time table to parquet files partitioned by year and month
#     time_table
#
#     # read in song data to use for songplays table
#     song_df =
#
#     # extract columns from joined song and log datasets to create songplays table
#     songplays_table =
#
#     # write songplays table to parquet files partitioned by year and month
#     songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    input_data = "data/"
    output_data = "outputs"
    
    process_song_data(spark, input_data, output_data)    
    # process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
