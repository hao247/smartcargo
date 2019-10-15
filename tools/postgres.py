import psycopg2 as pg


"""
functions for postgresql operations
"""


def update_psql(query, credent):
    """
    update table in postgresql
    :type query:    str     psql query for updating table
    :type credent:  dict    postgresql credentials
    """
    conn = pg.connect(
        host=credent["psql"]["host"],
        database=credent["psql"]["dbname"],
        user=credent["psql"]["user"],
        password=credent["psql"]["passwd"],
    )
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


def fetch_from_psql(query, credent):
    """
    fetch data from postgresql
    :type query:    str     psql fetching query
    :type credent:  dict    postgresql credentials
    :rtype:         list    list of result tuples
    """
    conn = pg.connect(
        host=credent["psql"]["host"],
        database=credent["psql"]["dbname"],
        user=credent["psql"]["user"],
        password=credent["psql"]["passwd"],
    )
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


def create_tables(table_list, credent):
    """
    create tables and convert trip_log table into hypertable:
    ship_info:  ship specs
    trips:      start and end port of trips
    trip_log:   trips tracks
    port_heat:  port traffic heat map
    :type table_list:   list    list of table names
    :type credent:      dict    postgresql credentials
    """
    for table in table_list.keys():
        fields = ",".join(
            field + " " + table_list[table]["fields"][field]
            for field in table_list[table]["order"]
        )
        query = "create table if not exists {} ({})".format(table, fields)
        update_psql(query, credent)

    create_hypertable = (
        "select create_hypertable('trip_log', 'basedatetime', if_not_exists => TRUE)"
    )
    update_psql(create_hypertable)


def drop_tables(table_list, credent):
    """
    drop existing tables, for debugging only
    :type table_list:   list    list of table names
    :type credent:      dict    postgresql credentials
    """
    update_psql("drop table if exists ship_info, trips")
    update_psql("drop table if exists trip_log")
