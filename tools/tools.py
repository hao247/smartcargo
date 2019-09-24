import sys
sys.path.append("../batch_processing/")

from batch_processing import BatchTransformer
import gmplot
from datetime import datetime
from pyspark.sql.functions import lag, unix_timestamp, udf, row_number, lit, when
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


def label_trip(df, mmsi):
    '''
    label different trips for a single ship in a dataframe.
    '''
    sog_thres = 0.1
    delta_time_thres = 10800  # 3 hours
    df = df.filter(df.MMSI == mmsi)\
            .filter(df.SOG > sog_thres)\
            .withColumn("PrevTime", lag(df.BaseDateTime).over(Window.partitionBy("MMSI").orderBy("BaseDateTime")))
    
    df = df.withColumn("DeltaTime", unix_timestamp(df.BaseDateTime) - unix_timestamp(df.PrevTime))\
            .withColumn("RowNumber", row_number().over(Window.partitionBy("MMSI").orderBy("BaseDateTime")))\
            .withColumn("TripID", lit(0))
    
    stop_df = df.filter(df.DeltaTime > delta_time_thres).select('RowNumber')

    row_start = 1
    for row in stop_df.collect():
        row_end = row['RowNumber'] - 1
        df = df.withColumn("TripID", when(df.RowNumber >= row_start, hash(str(mmsi)+str(row_start))).otherwise(df.TripID))
        row_start = row_end + 1
    df = df.withColumn("TripID", when(df.RowNumber >= row_start, hash(str(mmsi)+str(row_start))).otherwise(df.TripID))
    
    df.show(1000)
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
    
    #show_mmsi(df)
    #log = generate_ship_log(df, 565500000)
    #output_ship_log(log, 565500000)
    #output_trip_route(log, 565500000)
    
    trip_id = 1
    label_trip(df, 366940480)

if __name__ == '__main__':
    main()
