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


def find_trips(start, end):
    """
    find trips with specified start and end points
    :type start : list [lat, lon, range]
    :rtype      : list of records
    """
    query = 'select * from trips where "lat_start" >={} and "lat_start" <= {} and "lon_start" >= {} and "lon_start" <= {} and "lat_end" >= {} and "lat_end" <= {} and "lon_end" >= {} and "lon_end" <= {}'\
            .format(start[0]-start[2], start[0]+start[2], start[1]-start[2], start[1]+start[2], end[0]-end[2], end[0]+end[2], end[1]-end[2], end[1]+end[2])
    trips = fetch_from_psql(query)
    return trips


