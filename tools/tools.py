import sys
sys.path.append("../batch_processing/")
import gmplot
from datetime import datetime
import pyspark.sql.functions as f
import pyspark.sql.types as types
from pyspark.sql.window import Window
import yaml
import pygeohash as gh


def label_trip(df):
    '''
    label different trips for a single ship in a dataframe.
    '''
    sog_thres = 0.1
    diff_time_thres = 28800  # 8 hours

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
    loc_diff_thres = 0.1  # degrees in lat/lon, 0.01 is roughly 1km
    
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
    
    df = df.filter((f.abs(df.LAT_END-df.LAT_START) > loc_diff_thres)\
            & (f.abs(df.LON_END-df.LON_START) > loc_diff_thres))
    return df


def hash_port_location(ports):
    ports_hashed = {}
    for name in ports.keys():
        ports_hashed[name] = gh.encode(ports[name]['LAT'], ports[name]['LON'], 5)
        ports_hashed = dict(zip(ports_hashed.values(), ports_hashed.keys()))
    return ports_hashed


def label_destination(df):
    ports = yaml.load(open('../config/port_list.yaml', 'r'))
    ports_hashed = hash_port_location(ports)
    geohash_udf = f.udf(lambda x, y: gh.encode(x, y, 5), types.StringType())
    df = df.withColumn('DES', geohash_udf(df.LAT_END, df.LON_END))
    df = df.na.replace(ports_hashed, 'DES')
    return df


def list_mmsi(df):
    '''
    generate all distinct ship mmsi from a dataframe.
    '''
    ships = df.select("MMSI").distinct().collect()
    ship_list = [row['MMSI'] for row in ships]
    return ship_list


def generate_ship_log(df, mmsi):
    '''
    generate a single ship log, for data study only.
    '''
    datetime, lat, lon, sog, cog = [], [], [], [], []
    log = df.filter(df.MMSI == mmsi)\
            .orderBy('BaseDateTime')
    for row in log.collect():
        datetime.append(row['BaseDateTime'])
        lat.append(row['LAT'])
        lon.append(row['LON'])
        sog.append(row['SOG'])
        cog.append(row['COG'])
    
    log = {'datetime': datetime, 'lat':lat, 'lon':lon, 'sog':sog, 'cog':cog}
    return log


def output_ship_log(log, mmsi):
    '''
    output a single ship log file, for data study only.
    '''
    [datetime, lat, lon, sog, cog] = [log['datetime'], log['lat'], log['lon'], log['sog'], log['cog']]
    with open('./{}.txt'.format(mmsi), 'w+') as f:
        for i in range(len(datetime)):
            f.write('{}, {}, {}, {}, {}\n'\
                    .format(datetime[i], lat[i], lon[i], sog[i], cog[i]))


def output_trip_route(log, mmsi):
    '''
    output a single ship route, for data study only.
    '''
    [lat, lon] = [log['lat'], log['lon']]
    center = [sum(lat)/len(lat), sum(lon)/len(lon)]
    gmap3 = gmplot.GoogleMapPlotter(center[0], center[1], 5)
    gmap3.plot(lat, lon, 'red')
    gmap3.draw('./{}.html'.format(mmsi))


