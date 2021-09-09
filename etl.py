import configparser
from datetime import datetime
from schema import song_schema, log_schema
from pyspark.sql.types import DataType, TimestampType, DateType
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


# we are parsing aws key and secret key from dl.cfg file with using ConfigParser
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# we are creating spark session to operate spark operations
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()
    return spark

# this function is formatting ts for our time table 
def format_ts(ts):

    return datetime.fromtimestamp(ts / 1000.0)


def process_song_data(spark, input_data, output_data):
    # we are defining the song files path to create whole path that udacity files are.
    song_path = 'song_data/*/*/*/*.json'

    # get filepath to song data file
    song_data = input_data + song_path
    
    # we are getting schema from schema.py file that contains song and log files schema

    schema=song_schema()

    # read song data file
    df = spark.read.json(song_data,schema)

    # to check schema of song data loaded
    df.printSchema()

    # data sample of loaded data
    df.show(3)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    # load some data sample
    songs_table.show(3)

    # before writing , dropping duplicates to make song table proper
    songs_table = songs_table.drop_duplicates(subset=['title'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_latitude', 'artist_longitude', 'artist_location', 'artist_name')

    # checking some data sample 
    artists_table.show(3)
    
    # dropping duplicates before writing
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):

    # log file path
    log_path='log_data/*/*/*.json'

    # get filepath to log data file
    log_data = os.path.join(input_data, log_path)

    # log table scheme
    schema = log_schema

    # read log data file
    df = spark.read.json(log_data,schema)
    
    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.dropDuplicates('userId')
    
    # write users table to parquet files
    users_table.parquet(os.path.join(output_data, 'users'),mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda dt: format_ts(int(dt)), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda dt: format_ts(int(dt)), DateType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    df = df.withColumn('day_of_week', date_format(col('timestamp'), 'E'))
    time_table = df.select(col('timestamp').alias('start_time'),
                           hour(df.timestamp).alias('hour'),
                           dayofmonth(df.timestamp).alias('day'),
                           weekofyear(df.timestamp).alias('week'),
                           month(df.timestamp).alias('month'),
                           year(df.timestamp).alias('year'),
                           'day_of_week')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data, 'time'))


    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/A/A/A/*.json')


    # creating tables for spark sql 
    df.createOrReplaceTempView('d_log_table')
    song_df.createOrReplaceTempView('d_song_table')



    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                            SELECT
                                dl.timestamp AS start_time,
                                dl.userId,
                                dl.level,
                                ds.song_id,
                                ds.artist_id,
                                dl.sessionId,
                                dl.location,
                                dl.userAgent
                            FROM d_log_table dl, song_data_table ds                          
                            where dl.song = ds.title
                            ''')
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "enter your output_data bucket"
    
    # these functions are process_song_data and process_log_data are accepting spark session ,  s3 file path as input_data accepting output_data which is path of where we are going to write our result 
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    

if __name__ == "__main__":
    main()
