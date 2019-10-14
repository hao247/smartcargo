from pyspark.ml.feature import Bucketizer
import pyspark.sql.functions as f
import pandas as pd

def label_scores(df, lat, lon, rang):
    lat_splits = [-90.0, lat-rang, lat+rang, 90.0]
    lon_splits = [-180.0, lon-rang, lon+rang, 180.0]
    bucketizer_lat_start = Bucketizer(splits=lat_splits, inputCol="lat_start", outputCol="LAT_START_SCORE")
    bucketizer_lon_start = Bucketizer(splits=lon_splits, inputCol="lon_start", outputCol="LON_START_SCORE")
    bucketizer_lat_end = Bucketizer(splits=lat_splits, inputCol="lat_end", outputCol="LAT_END_SCORE")
    bucketizer_lon_end = Bucketizer(splits=lon_splits, inputCol="lon_end", outputCol="LON_END_SCORE")
    
    df = bucketizer_lat_start.transform(df)
    df = bucketizer_lon_start.transform(df)
    df = bucketizer_lat_end.transform(df)
    df = bucketizer_lon_end.transform(df)
    df = df.withColumn('SCORE_START', df.LAT_START_SCORE*df.LON_START_SCORE)
    df = df.withColumn('SCORE_END', df.LAT_END_SCORE*df.LON_END_SCORE)

    return df

def label_ports(df):
    rang = 0.05
    ports = pd.read_csv('s3://hao-zheng-databucket/ports/Major_Ports.csv')
    df = df.withColumn('PORT_START', f.lit('null'))
    df = df.withColumn('PORT_END', f.lit('null'))
    for index, port in ports.iterrows():
        lat = port['Y']
        lon = port['X']
        port_name = port['PORT_NAME']
        df = label_scores(df, lat, lon, rang)
        df = df.withColumn('PORT_START', f.when(df.SCORE_START == 1.0, port_name).otherwise(df.PORT_START))
        df = df.withColumn('PORT_END', f.when(df.SCORE_END == 1.0, port_name).otherwise(df.PORT_END))
        drop_scores = ["LAT_START_SCORE", "LON_START_SCORE", "LAT_END_SCORE", "LON_END_SCORE", "SCORE_START", "SCORE_END"]
        df = df.drop(*drop_scores)

    drop_coordinates = ["lat_start", "lon_start", "lat_end", "lon_end"]
    df = df.drop(*drop_coordinates)
    return df
