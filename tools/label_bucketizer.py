from pyspark.ml.feature import Bucketizer
import pyspark.sql.functions as f
import pandas as pd

def bin_location(df, lat, lon, rang, input_cols):
    lat_splits = [-90.0, lat-rang, lat+rang, 90.0]
    lon_splits = [-180.0, lon-rang, lon+rang, 180.0]

    for input_col in input_cols:
        output_col = input_col + "_SCORE"
        if input_col[:3] == "LAT":
            splits = lat_splits
        else:
            splits = lon_splits
        bucketizer = Bucketizer(splits=splits, inputCol=input_col, outputCol = output_col)
        df = bucketizer.transform(df)
    
    df = df.withColumn('SCORE_START', df.LAT_START_SCORE*df.LON_START_SCORE)
    df = df.withColumn('SCORE_END', df.LAT_END_SCORE*df.LON_END_SCORE)
    return df

def label_ports(df, rang):
    ports = pd.read_csv('s3://hao-zheng-databucket/ports/Major_Ports.csv')
    drop_scores = ["LAT_START_SCORE", "LON_START_SCORE", "LAT_END_SCORE",\
            "LON_END_SCORE", "SCORE_START", "SCORE_END"]
    input_cols = ["LAT_START", "LON_START", "LAT_END", "LON_END"]
    df = df.withColumn('PORT_START', f.lit('null'))
    df = df.withColumn('PORT_END', f.lit('null'))
    
    for index, port in ports.iterrows():
        lat = port['Y']
        lon = port['X']
        port_name = port['PORT_NAME']
        df = bin_location(df, lat, lon, rang, input_cols)
        df = df.withColumn('PORT_START', f.when(df.SCORE_START == 1.0, port_name).otherwise(df.PORT_START))
        df = df.withColumn('PORT_END', f.when(df.SCORE_END == 1.0, port_name).otherwise(df.PORT_END))
        df = df.drop(*drop_scores)

    df = df.drop(*input_cols)
    return df

def port_heat_map(df, rang):
    ports = pd.read_csv('s3://hao-zheng-databucket/ports/Major_Ports.csv')
    input_cols = ["LAT", "LON"]
    scores = ["LAT_SCORE", "LON_SCORE"]
    df = df.sample(0.02)
    df = df.drop("MMSI").drop("BaseDateTime")
    df = df.withColumn('PORT', f.lit('null'))

    for index, port in ports.iterrows():
        lat = port['Y']
        lon = port['X']
        port_name = port['PORT_NAME']
        lat_splits = [-90.0, lat-rang, lat+rang, 90.0]
        lon_splits = [-180.0, lon-rang, lon+rang, 180.0]

        for input_col in input_cols:
            output_col = input_col + "_SCORE"
            if input_col[:3] == "LAT":
                splits = lat_splits
            else:
                splits = lon_splits
            bucketizer = Bucketizer(splits=splits, inputCol=input_col, outputCol = output_col)
            df = bucketizer.transform(df)
        
        df = df.withColumn('SCORE', df.LAT_SCORE*df.LON_SCORE)
        df = df.withColumn('PORT', f.when(df.SCORE ==1.0, port_name).otherwise(df.PORT))
        df = df.drop(*scores)
    df = df.drop("SCORE")
    df = df.filter(df.PORT != 'null')
    return df
