import label_bucketizer as label
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
from pyspark.sql import SQLContext
import label_b as label
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, StringType
import time

time_start = time.time()
url = 'jdbc:postgresql://ip-10-0-0-12:5432/postgres'
table = 'trips_label_bucketizer_test'
user = 'postgres'
passwd = 'sunwind4U!'
spark = SparkSession.builder.getOrCreate()
        
df = spark.read\
        .format("jdbc")\
        .option("url", url)\
        .option("dbtable", table)\
        .option("user", user)\
        .option("password", passwd)\
        .option("driver", "org.postgresql.Driver")\
        .load()
df.show(10)
df = label.label_ports(df)
time_end = time.time()
df.show(50)
print("Total time: {}".format(time_end - time_start))
