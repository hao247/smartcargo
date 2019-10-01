import sys
sys.path.append('../tools')
import math
import psycopg2
import yaml


def fetch_from_psql(query):
    """
    query data from PostgreSQL database
    """
    credent = yaml.load(open('../config/credentials.yaml', 'r'))
    conn = psycopg2.connect(\
            host=credent['psql']['host'],\
            database=credent['psql']['dbname'],\
            user=credent['psql']['user'],\
            password=credent['psql']['passwd']\
            )
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


def find_trips(end):
    """
    find trips with specified start and end points
    :type start : list [lat, lon, range]
    :rtype      : list of records
    """
    query = 'select * from trips where "lat_end" >= {} and "lat_end" <= {} and "lon_end" >= {} and "lon_end" <= {}'\
            .format(end[0]-end[2], end[0]+end[2], end[1]-end[2], end[1]+end[2])
    trips = fetch_from_psql(query)
    return trips


def find_trip_log(trip):
    time_start, time_end, mmsi = str(trip[1]), str(trip[3]), str(trip[2])
    query = 'select * from trip_log where "mmsi" = {} and "basedatetime" >= \'{}\' and "basedatetime" <= \'{}\''.format(mmsi, time_start, time_end)
    trip_log = fetch_from_psql(query)
    return trip_log


def find_trips_port_to_port(port_start, port_end):
    """
    find all trips with specified start- and end-ports
    """
    ports = yaml.load(open('../config/port_list.yaml', 'r'))
    start  = [ports[port_start]['LAT'], ports[port_start]['LON'], ports[port_start]['RANGE']]
    end  = [ports[port_end]['LAT'], ports[port_end]['LON'], ports[port_end]['RANGE']]
    trips = find_trips(end)


def find_trips_loc_to_port(lat_now, lon_now, port_end):
    """
    find all trips with specified current location and end-port
    """
    ports = yaml.load(open('../config/port_list.yaml', 'r'))
    start  = [lat_now, lon_now, 0.04]
    end  = [ports[port_end]['LAT'], ports[port_end]['LON'], ports[port_end]['RANGE']]
    return find_trips(end)

