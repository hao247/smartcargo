import sys
sys.path.append("../batch_processing/")

from batch_processing import BatchTransformer
import gmplot


def show_ships(df):
    shipList = df.select("MMSI").distinct().collect()
    print("ship_MMSI:")
    for r in shipList:
        print(r["MMSI"])


def show_trip(df, mmsi):
    lat = []
    lon = []
    trip = df.filter(df.MMSI == mmsi)\
            .orderBy("BaseDateTime")\
            .select('LAT', 'LON')
    lat = [row['LAT'] for row in trip.collect()]
    lon = [row['LON'] for row in trip.collect()]
    center = [sum(lat)/len(lat), sum(lon)/len(lon)]
    gmap3 = gmplot.GoogleMapPlotter(center[0], center[1], 5)
    gmap3.plot(lat, lon, 'red')
    gmap3.draw('./{}.html'.format(mmsi))


def main():
    s3_configfile = '../config/s3bucket.config'
    raw_data_fields_configfile = '../config/raw_data_fields.config'
    schema_configfile = '../config/trip_schema.config'
    psql_configfile = '../config/psql.config'
    credentfile = '../config/credentials.txt'
    trips = BatchTransformer(s3_configfile, raw_data_fields_configfile, psql_configfile, schema_configfile, credentfile)
    trips.read_from_s3()
    df = trips.df
    #show_ships(df)
    show_trip(df, 247009600)

if __name__ == '__main__':
    main()
