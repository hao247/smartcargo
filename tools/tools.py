from datetime import datetime
import pyspark.sql.functions as f
import pyspark.sql.types as types
from pyspark.sql.window import Window
import yaml
import psycopg2 as pg
import pandas as pd
import datetime
import boto3




def group_trips(df):
    """
    label different trips in a dataframe.
    :type df:   spark dataframe     dataframe containing timestamps
                                    of ship locations
    :rtype:     spark dataframe     dataframe with trip starting and
                                    ending time stamp labeled
    """
    sog_thres = 0.1
    diff_time_thres = 10800 # 3 hours

    window = Window.partitionBy("MMSI").orderBy("BaseDateTime")
    df = df.filter(df.SOG > sog_thres)\
            .withColumn("PrevTime", f.lag(df.BaseDateTime).over(window))
    
    df = df.withColumn("DiffTime", f.unix_timestamp(df.BaseDateTime)
                       - f.unix_timestamp(df.PrevTime))

    df = df.withColumn('TripID', f.when((df.DiffTime > diff_time_thres) |
                                        (f.isnull(df.DiffTime)),
                                        f.monotonically_increasing_id())
                       .otherwise(f.lit(None)))\
                       .select("MMSI", "BaseDateTime", "LAT", "LON", "TripID")
    return df


def gen_trip_table(df):
    """
    generate trip table from a labeled dataframe
    :type df:   spark dataframe     labeled dataframe from group_trips
    :rtype:     spark dataframe     a new data frame of trips
    """
    time_diff_thres = 7200 # trip is longer than 2 hours
    
    window = Window.partitionBy("MMSI").orderBy("BaseDateTime")
    window_desc = Window.partitionBy("MMSI").orderBy(f.desc("BaseDateTime"))
    lag_labels = ['BaseDateTime', 'LAT', 'LON']
    lead_labels = ['PREV_BaseDateTime', 'PREV_LAT', 'PREV_LON']
    
    df = df.select(*[c for c in df.columns],
                   *[f.first(f.col(col_name)).over(window_desc)
                     .alias(col_name + '_END') for col_name in lag_labels],
                   *[f.lag(f.col(col_name)).over(window).alias('PREV_' + col_name)
                     for col_name in lag_labels])\
           .filter(df.TripID.isNotNull())
    
    df = df.select(*[c for c in df.columns],
                   *[f.lead(f.col(col_name)).over(window)
                     .alias('LAST_' + col_name) for col_name in lead_labels])

    df = df.withColumn('BaseDateTime_END',
                       f.when(~(f.isnull(df.LAST_PREV_BaseDateTime)),
                       df.LAST_PREV_BaseDateTime).otherwise(df.BaseDateTime_END))\
           .withColumn('LAT_END', f.when(~(f.isnull(df.LAST_PREV_LAT)),
                       df.LAST_PREV_LAT).otherwise(df.LAT_END))\
           .withColumn('LON_END', f.when(~(f.isnull(df.LAST_PREV_LON)),
                       df.LAST_PREV_LON).otherwise(df.LON_END))\
           .select("TripID", "MMSI",
                   f.col("BaseDateTime").alias("TIME_START"),
                   f.col('BaseDateTime_END').alias("TIME_END"),
                   f.col("LAT").alias("LAT_START"),
                   f.col("LON").alias("LON_START"),
                   "LAT_END","LON_END")
    
    df = df.filter(f.unix_timestamp(df.TIME_END)-f.unix_timestamp(df.TIME_START) >
                   time_diff_thres)
    
    return df


def label_ships(trip_table, ship_table):
    """
    A optional tool for labeling ship_info in trips_final stored in psql
    :type trip_table:   str     name of trip table
    :type ship_table:   str     name of ship table
    :rtype:             pandas dataframe    list of trips with ship type labeled
    """
    credent = yaml.load(open('../config/credentials.yaml', 'r'))
    conn = pg.connect(host=credent['psql']['host'],
                      database=credent['psql']['dbname'],
                      user=credent['psql']['user'],
                      password=credent['psql']['passwd'])
    trips = pd.read_sql_query("select * from {}".format(trip_table), con=conn)
    ships = pd.read_sql_query("select * from {}".format(ship_table), con=conn)
    pd.merge(trips, ships, left_on='mmsi')
    return pd

