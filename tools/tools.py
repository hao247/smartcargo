import sys
sys.path.append("../batch_processing/")
from datetime import datetime
import pyspark.sql.functions as f
import pyspark.sql.types as types
from pyspark.sql.window import Window
import yaml
import psycopg2
import datetime


def label_trip(df):
    '''
    label different trips for a single ship in a dataframe.
    '''
    sog_thres = 0.2
    diff_time_thres = 57600 # 16 hours

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
    generate trip table from a labelled dataframe
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


def fetch_from_psql(query):
    """
    query data from PostgreSQL database
    """
    credent = yaml.load(open('/home/ubuntu/git/smartcargo/config/credentials.yaml', 'r'))
    conn = psycopg2.connect(\
            host=credent['psql']['host'],\
            database=credent['psql']['dbname'],\
            user=credent['psql']['user'],\
            password=credent['psql']['passwd'])
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


def send_to_psql(query):
    """
    query data from PostgreSQL database
    """
    credent = yaml.load(open('/home/ubuntu/git/smartcargo/config/credentials.yaml', 'r'))
    conn = psycopg2.connect(\
            host=credent['psql']['host'],\
            database=credent['psql']['dbname'],\
            user=credent['psql']['user'],\
            password=credent['psql']['passwd'])
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


def label_trips():
    credent = yaml.load(open('/home/ubuntu/git/smartcargo/config/credentials.yaml', 'r'))
    conn = psycopg2.connect(\
            host=credent['psql']['host'],\
            database=credent['psql']['dbname'],\
            user=credent['psql']['user'],\
            password=credent['psql']['passwd'])
    cursor = conn.cursor()
    cursor.execute('alter table trips_test add column if not exists departure text')
    cursor.execute('alter table trips_test add column if not exists arrival text')
    cursor.execute('alter table trips_test add column if not exists duration double precision')
    port_list = yaml.load(open('/home/ubuntu/git/smartcargo/config/port_list.yaml', 'r'))
    trips = fetch_from_psql('select * from trips')
    for trip in trips:
        print(trip)
        [tripid, mmsi, time_start, time_end, lat_start, lat_end, lon_start, lon_end] = trip
        duration = time_end-time_start
        duration = duration.total_seconds()/3600
        port_start = None
        port_end = None
        for port in port_list:
            lat, lon, rang = port_list[port]['LAT'], port_list[port]['LON'], port_list[port]['RANGE']
            if lat-rang < lat_start < lat+rang and lon-rang < lon_start < lon+rang:
                port_start = port
            if lat-rang < lat_end < lat+rang and lon-rang < lon_end < lon+rang:
                port_end = port
        cursor.execute('insert into trips_test (tripid, mmsi, time_start, time_end, lat_start, lat_end, lon_start, lon_end, departure, arrival, duration) values (\'{}\', {}, \'{}\', \'{}\', {}, {}, {}, {}, \'{}\', \'{}\', {})'.format(tripid, mmsi, time_start, time_end, lat_start, lat_end, lon_start, lon_end, port_start, port_end, duration))
    conn.commit()
    cursor.close()
    conn.close()
