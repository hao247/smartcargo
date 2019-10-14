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
    """
    sog_thres = 0.1
    diff_time_thres = 10800 # 3 hours

    window = Window.partitionBy("MMSI").orderBy("BaseDateTime")
    df = df.filter(df.SOG > sog_thres)\
            .withColumn("PrevTime", f.lag(df.BaseDateTime).over(window))
    
    df = df.withColumn("DiffTime", f.unix_timestamp(df.BaseDateTime) - f.unix_timestamp(df.PrevTime))

    df = df.withColumn('TripID', f.when((df.DiffTime > diff_time_thres) | (f.isnull(df.DiffTime)),\
            f.monotonically_increasing_id()).otherwise(f.lit(None)))\
            .select("MMSI", "BaseDateTime", "LAT", "LON", "TripID")
    return df


def gen_trip_table(df):
    """
    generate trip table from a labeled dataframe
    """
    time_diff_thres = 7200 # trip is longer than 2 hours
    
    window = Window.partitionBy("MMSI").orderBy("BaseDateTime")
    window_desc = Window.partitionBy("MMSI").orderBy(f.desc("BaseDateTime"))
    lag_labels = ['BaseDateTime', 'LAT', 'LON']
    lead_labels = ['PREV_BaseDateTime', 'PREV_LAT', 'PREV_LON']
    
    df = df.select(*[c for c in df.columns],\
            *[f.first(f.col(col_name)).over(window_desc).alias(col_name + '_END') for col_name in lag_labels],\
            *[f.lag(f.col(col_name)).over(window).alias('PREV_' + col_name) for col_name in lag_labels])\
            .filter(df.TripID.isNotNull())
    
    df = df.select(*[c for c in df.columns],\
            *[f.lead(f.col(col_name)).over(window).alias('LAST_' + col_name) for col_name in lead_labels])

    df = df.withColumn('BaseDateTime_END', f.when(~(f.isnull(df.LAST_PREV_BaseDateTime)), df.LAST_PREV_BaseDateTime).otherwise(df.BaseDateTime_END))\
            .withColumn('LAT_END', f.when(~(f.isnull(df.LAST_PREV_LAT)), df.LAST_PREV_LAT).otherwise(df.LAT_END))\
            .withColumn('LON_END', f.when(~(f.isnull(df.LAST_PREV_LON)), df.LAST_PREV_LON).otherwise(df.LON_END))\
            .select("TripID", "MMSI", f.col("BaseDateTime").alias("TIME_START"), f.col('BaseDateTime_END').alias("TIME_END"),\
            f.col("LAT").alias("LAT_START"), f.col("LON").alias("LON_START"), "LAT_END", "LON_END")
    
    df = df.filter(f.unix_timestamp(df.TIME_END)-f.unix_timestamp(df.TIME_START) > time_diff_thres)
    
    return df


#def label_ports():
#    rang = 0.5
#    ports = pd.read_csv('s3://hao-zheng-databucket/ports/Major_Ports.csv')
#    credent = yaml.load(open('../config/credentials.yaml', 'r'))
#    conn = pg.connect(\
#            host=credent['psql']['host'],\
#            database=credent['psql']['dbname'],\
#            user=credent['psql']['user'],\
#            password=credent['psql']['passwd'])
#    cursor = conn.cursor()
#    trips = pd.read_sql_query("select * from trips", con=conn)
#    ports = pd.read_csv('s3://hao-zheng-databucket/ports/Major_Ports.csv')
#    trips['duration'] = trips['time_end'] - trips['time_start']
#    trips['duration'] = trips['duration'].dt.total_seconds() / 3600
#    trips['departure'] = ''
#    trips['arrival'] = ''
#    for index, port in ports.iterrows():
#        lat = port['Y']
#        lon = port['X']
#        lat_bin = [-90, lat-rang, lat+rang, 90]
#        lon_bin = [-180, lon-rang, lon+rang, 180]
#        print(lat_bin, lon_bin)
#        scores = [0, 4, 2]
#        trips['lat_start_score'] = pd.cut(trips['lat_start'], lat_bin, labels=scores).astype(int)
#        trips['lon_start_score'] = pd.cut(trips['lon_start'], lon_bin, labels=scores).astype(int)
#        trips['lat_end_score'] = pd.cut(trips['lat_end'], lat_bin, labels=scores).astype(int)
#        trips['lon_end_score'] = pd.cut(trips['lon_end'], lon_bin, labels=scores).astype(int)
#        trips['start_score'] = trips['lat_start_score'] + trips['lon_start_score']
#        trips['end_score'] = trips['lat_end_score'] + trips['lon_end_score']
#        total_bin= [-1, 7 ,10]
#        total_scores = ['', port['PORT_NAME']]
#        trips['departure_tmp'] = pd.cut(trips['start_score'], total_bin, labels=total_scores).astype(str)
#        trips['arrival_tmp'] = pd.cut(trips['end_score'], total_bin, labels=total_scores).astype(str)
#        trips['departure'] = trips['departure'].str.cat(trips['departure_tmp']).str.strip()
#        trips['arrival'] = trips['arrival'].str.cat(trips['arrival_tmp']).str.strip()
#    trips.drop(['lat_start_score', 'lon_start_score', 'lat_end_score', 'lon_end_score', 'departure_tmp', 'arrival_tmp', 'start_score', 'end_score'], axis=1)
#    for index, row in trips.iterrows():
#        cursor.execute('insert into trips_labeled (tripid, mmsi, time_start, time_end, lat_start, lat_end, lon_start, lon_end, departure, arrival, duration) values (\'{}\', {}, \'{}\', \'{}\', {}, {}, {}, {}, \'{}\', \'{}\', {})'.format(row['tripid'], row['mmsi'], row['time_start'], row['time_end'], row['lat_start'], row['lat_end'], row['lon_start'], row['lon_end'], row['departure'], row['arrival'], row['duration']))
#        conn.commit()

def label_ships():
    credent = yaml.load(open('../config/credentials.yaml', 'r'))
    conn = pg.connect(\
            host=credent['psql']['host'],\
            database=credent['psql']['dbname'],\
            user=credent['psql']['user'],\
            password=credent['psql']['passwd'])
    trips = pd.read_sql_query("select * from trips_final", con=conn)
    ships = pd.read_sql_query('select * from ship_info', con=conn)
    pd.merge(trips, ships, left_on='mmsi')
    print(trips)
    print(ships)


