import sys
sys.path.append("../batch_processing/")

from batch_processing import BatchTransformer
import gmplot
from datetime import datetime
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, BooleanType


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


def label_trip(df):
    '''
    label different trips for a single ship in a dataframe.
    '''
    sog_thres = 0.1
    delta_time_thres = 21600  # 6 hours
    df = df.filter(df.SOG > sog_thres)\
            .withColumn("PrevTime", f.lag(df.BaseDateTime).over(Window.partitionBy("MMSI").orderBy("BaseDateTime")))\
    
    df = df.withColumn("DeltaTime", f.unix_timestamp(df.BaseDateTime) - f.unix_timestamp(df.PrevTime))\
            .withColumn("RowNumber", f.row_number().over(Window.partitionBy("MMSI").orderBy("BaseDateTime")))

    df = df.withColumn('TripID', f.when((df.DeltaTime > delta_time_thres) | (f.isnull(df.DeltaTime)), f.hash(df.MMSI + df.RowNumber)).otherwise(f.lit(0)))\
            .select("MMSI", "BaseDateTime", "LAT", "LON", "SOG", "Cargo", "RowNumber", "TripID")
   
    return df


def gen_trip_table(df):
    """
    generate trip table from a labelled dataframe
    """
    loc_diff_thres = 0.01  # degrees in lat/lon, 0.01 is roughly 1km
    df = df.withColumn('LastRowPrevTrip', f.lag(df.RowNumber).over(Window.partitionBy("MMSI").orderBy("RowNumber")))\
            .withColumn('LastLATPrevTrip', f.lag(df.LAT).over(Window.partitionBy("MMSI").orderBy("RowNumber")))\
            .withColumn('LastLONPrevTrip', f.lag(df.LON).over(Window.partitionBy("MMSI").orderBy("RowNumber")))\
            .withColumn('RowEnd', f.max(df.RowNumber).over(Window.partitionBy("MMSI")))\
            .withColumn('LAT_END', f.last(df.LAT).over(Window.partitionBy("MMSI")))\
            .withColumn('LON_END', f.last(df.LON).over(Window.partitionBy("MMSI")))\
            .filter(df.TripID != 0)

    df = df.withColumn('LastRow', f.lead(df.LastRowPrevTrip).over(Window.partitionBy("MMSI").orderBy("RowNumber")))\
            .withColumn('LastLAT', f.lead(df.LastLATPrevTrip).over(Window.partitionBy("MMSI").orderBy("RowNumber")))\
            .withColumn('LastLON', f.lead(df.LastLONPrevTrip).over(Window.partitionBy("MMSI").orderBy("RowNumber")))
            
    df = df.withColumn('RowEnd', f.when(~(f.isnull(df.LastRow)), df.LastRow).otherwise(df.RowEnd))\
            .withColumn('LAT_END', f.when(~(f.isnull(df.LastLAT)), df.LastLAT).otherwise(df.LAT_END))\
            .withColumn('LON_END', f.when(~(f.isnull(df.LastLON)), df.LastLON).otherwise(df.LON_END))\
            .select("TripID", "MMSI", f.col("RowNumber").alias("RowStart"), "RowEnd", f.col("LAT").alias("LAT_START"), f.col("LON").alias("LON_START"), "LAT_END", "LON_END")
    
    df = df.filter((f.abs(df.LAT_END-df.LAT_START) > loc_diff_thres) & (f.abs(df.LON_END-df.LON_START) > loc_diff_thres))
    return df


def main():
    s3_configfile = '../config/s3bucket.config'
    raw_data_fields_configfile = '../config/raw_data_fields.config'
    schema_configfile = '../config/trip_schema.config'
    psql_configfile = '../config/psql.config'
    credentfile = '../config/credentials.txt'
    trips = BatchTransformer(s3_configfile, raw_data_fields_configfile, psql_configfile, schema_configfile, credentfile)
    trips.read_from_s3()
    df = trips.df


if __name__ == '__main__':
    main()
