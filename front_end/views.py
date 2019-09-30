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
    conn = pyscopg2.connect(\
            host=credent['psql']['host']\
            database=credent['psql']['dbname']\
            user=credent['psql']['user']\
            password=credent['psql']['passwd']\
            )
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


def find_trips(start, end):
    """
    find trips with specified start and end points
    :type start : list [lat, lon, range]
    :rtype      : list of records
    """
    query = 'select * from trips where "LAT_START" >= {} and "LAT_START" <= {} and "LON_START" >= {} and "LON_START" <= {} and "LAT_END" >= {} and "LAT_END" <= {} and "LON_END" >= {} and "LON_END" <= {}'\
            .format(start[0]-start[2], start[0]+start[2], start[1]-start[2], start[1]+start[2], end[0]-end[2], end[0]+end[2], end[1]-end[2], end[1]+end[2],)
    trips = fetch_from_psql(query)
    return trips


def find_trips_port_to_port(port_start, port_end):
    """
    find all trips with specified start- and end-ports
    """
    ports = yaml.load(open('../config/port_list.yaml', 'r'))
    start  = [ports[port_start]['LAT'], ports[port_start]['LON'], ports[port_start]['RANGE']]
    end  = [ports[port_end]['LAT'], ports[port_end]['LON'], ports[port_end]['RANGE']]
    return find_trips(start, end)


def find_trips_loc_to_port(lat_now, lon_now, port_end):
    """
    find all trips with specified current location and end-port
    """
    ports = yaml.load(open('../config/port_list.yaml', 'r'))
    start  = [lat_now, lon_now, 0.04]
    end  = [ports[port_end]['LAT'], ports[port_end]['LON'], ports[port_end]['RANGE']]
    return find_trips(start, end)

