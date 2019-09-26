import pygeohash as gh
import pyspark.sql.types as t
import pyspark.sql.functions as f
import yaml


def geohash_5(lat, lon):
    return gh.encode(lat, lon, 5)


def hash_port_location(ports):
    ports_hashed = {}
    for name in ports.keys():
        ports_hashed[name] = geohash_5(ports[name]['LAT'], ports[name]['LON'])
        ports_hashed = dict(zip(ports_hashed.values(), ports.keys()))
    return ports_hashed


def label_destination(df):
    ports = yaml.load(open('../config/port_list.yaml', 'r'))
    ports_hashed = hash_port_location(ports)
    geohash_5_UDF = f.udf(geohash_5, t.StringType())
    df = df.withColumn('Destination', geohash_5_UDF(df.LAT_END, df.LON_END))
    df = df.na.replace(ports_hashed, 'Destination')
    return df



