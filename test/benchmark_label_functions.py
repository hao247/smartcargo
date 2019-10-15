import label_bucketizer as lb
import label_pandas as lp
import label_sql as ls
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
from pyspark.sql import SQLContext
import time
import yaml


def label_benchmark():
    """
    a benchmark function for testing speeds of different labeling methods
    """
    credentfile = "../config/credentials.yaml"
    credent = yaml.load(open(credentfile, "r"))
    url = credent["psql"]["url"]
    table = crendent["benchmark"]["table"]
    user = credent["psql"]["user"]
    passwd = credent["psql"]["passwd"]
    spark = SparkSession.builder.getOrCreate()

    df = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", passwd)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    time_start = time.time()
    df_bucketizer = lb.label_ports(df, 0.05)
    time_end = time.time()
    print("label_bucketizer: {} s".format(time_end - time_start))

    time_start = time.time()
    ls.label_ports("trips_final", 2000)
    time_end = time.time()
    print("label_sql: {} s".format(time_end - time_start))

    time_start = time.time()
    df_pandas = lp.label_ports(0.05)
    time_end = time.time()
    print("label_pandas: {} s".format(time_end - time_start))


if __name__ == "__main__":
    label_benchmark()
