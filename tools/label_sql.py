import boto3
import yaml
import time
import re
import postgres as psql

"""
port labeling function by using sql queries
relatively slow, only for benchmarking
"""


def read_csv(bucketname, filename):
    """
    imports and splits csv file
    :type bucketname:   str     s3 data bucket name
    :type filename:     str     csv file name
    :rtype:             list    list of rows in csv file
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucketname)
    obj = bucket.Object(filename)
    response = obj.get()
    lines = response["Body"].read().decode("utf-8").splitlines()
    lines = lines[1:]
    return lines


def label_ports(table_name, rang):
    """
    labels trips with startging and ending port names
    :type table_name:   str     name of the trip table stored in postgresql
    :type rang:         float   range/size of the port in meters
    """
    credent = yaml.load(open("../config/credentials.yaml", "r"))
    ports = read_csv("hao-zheng-databucket", "ports/Major_Ports.csv")
    add_columns = "alter table {} add column if not exists departure text, add column if not exists arrival text, add column if not exists duration double precision".format(
        table_name
    )
    psql.update_psql(add_columns, credent)
    for row in ports:
        row_parsed = re.split(r",(?=[^\s])", row)
        port_name = row_parsed[5].strip('"')
        lat = float(row_parsed[1])
        lon = float(row_parsed[0])
        filter_start_location = 'update {} set "departure" = \'{}\'  where earth_distance(ll_to_earth("lat_start", "lon_start"), ll_to_earth({}, {})) < {}'.format(
            table_name, port_name, lat, lon, rang
        )
        filter_end_location = 'update {} set "arrival" = \'{}\'  where earth_distance(ll_to_earth("lat_end", "lon_end"), ll_to_earth({}, {})) < {}'.format(
            table_name, port_name, lat, lon, rang
        )
        psql.update_psql(filter_start_location, credent)
        psql.update_psql(filter_end_location, credent)
    drop_columns = "alter table {} drop column lat_start, drop column lon_start, dropcolumn lat_end, drop column lon_end, drop column mmsi".format(
        table_name
    )
    calculate_duration = "update {} set duration = extract(epoch from (time_end - time_start))/3600".format(
        table_name
    )
    psql.update_psql(drop_columns, credent)
    psql.update_psql(calculate_duration, credent)
