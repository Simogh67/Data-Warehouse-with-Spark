import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    ''' This function creates a spark session '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''This function creates songs and artists tables
       Args: spark: SparkSession, input_data: input files path
              output_data: results path'''
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    df =spark.read.json(song_data) 

    # extract columns to create songs table
    songs_table =df.select('song_id', 
                           'title', 
                           'artist_id',
                           'year',
                           'duration') \
                           .dropDuplicates(subset=['song_id']) 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              'artist_name',
                              'artist_location',
                              'artist_latitude',
                              'artist_longitude')\
                               .dropDuplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))                         


def process_log_data(spark, input_data, output_data):
    '''This function creates tables users, time, song_plays
        Args: spark: SparkSession, input_data: input files path
              output_data: results path'''
    # get filepath to log data file
    log_data =input_data + "log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df =df.filter(df.page=='NextSong') 

    # extract columns for users table    
    users_table =df.select('userId',
                            'firstName',
                            'lastName',
                            'gender',
                            'level')\
                             .dropDuplicates(subset=['userId']) 
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
   # get_timestamp = udf()
    df = df.withColumn('start_time',((df.ts.cast('float')/1000).cast("timestamp")) )
    
    # create datetime column from original timestamp column
   # get_datetime = udf()
   # df = 
    
    # extract columns to create time table
    time_table = df.selectExpr("start_time as start_time",
                               "hour(start_time) as hour",
                               "dayofmonth(start_time) as day",
                               "weekofyear(start_time) as week",
                               "month(start_time) as month",
                               "year(start_time) as year",
                               "dayofweek(start_time) as weekday")\
                                .dropDuplicates()
                               
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'time'))

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data,'song_data/*/*/*/*.json'))

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("temp_songs")
    df.createOrReplaceTempView("temp_logs")
    songplays_table = spark.sql("""SELECT monotonically_increasing_id() as songplay_id,
                                    start_time as start_time,
                                    year(start_time) as year,
                                    month(start_time) as month,
                                    userId,
                                    level,
                                    song_id,
                                    artist_id,
                                    sessionId,
                                    location,
                                    userAgent
                                    from temp_songs
                                    join temp_logs on temp_logs.artist = temp_songs.artist_name
                                    and temp_logs.page ='NextSong'""")
 
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'songplays'))


def main():
    '''This function create a spark session, read the logs and 
        songs data from S3, then creates tables users, songs, artists
         songs_paly'''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sia-udacity-dend-song_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
