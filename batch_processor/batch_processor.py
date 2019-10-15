import sys
sys.path.append("./tools/")
import tools
from pyspark.sql import SparkSession
import postgres as psql
import label_bucketizer as label
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, StringType
import yaml
import time


class BatchProcessor:
    """
    class that reads data from S3 bucket,
    process it by using spark
    and save results into postgresql/timescale database
    """


    def __init__(self, s3_configfile, schema_configfile, credentfile):
        """
        class constructor initializing the instance according to configuration files
        :type s3_configfile:        str     path to s3 config gile
        :type schema_configfile:    str     path to schema config file
        :type credentfile:          str     path to credentials file
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.credent = yaml.load(open(credentfile, 'r'))
        self.s3_conf = yaml.load(open(s3_configfile, 'r'))
        self.schema_conf = yaml.load(open(schema_configfile, 'r'))


    def read_from_s3(self):
        """
        reads files from s3 bucket and creates spark dataframe
        """
        s3file = 's3a://{}/{}/{}'.format(self.s3_conf['bucket'],
                  self.s3_conf['folder'], self.s3_conf['file'])

        schema = StructType([
            StructField("MMSI", IntegerType(), True),
            StructField("BaseDateTime", TimestampType(), True),
            StructField("LAT", DoubleType(), True),
            StructField("LON", DoubleType(), True),
            StructField("SOG", DoubleType(), True),
            StructField("COG", DoubleType(), True),
            StructField("Heading", DoubleType(), True),
            StructField("VesselName", StringType(), True),
            StructField("IMO", StringType(), True),
            StructField("CallSign", StringType(), True),
            StructField("VesselType", IntegerType(), True),
            StructField("Status", StringType(), True),
            StructField("Length", DoubleType(), True),
            StructField("Width", DoubleType(), True),
            StructField("Draft", DoubleType(), True),
            StructField("Cargo", IntegerType(), True)
            ])

        self.df = self.spark.read.csv(header=True, schema=schema, path=s3file)


    def save_to_psql(self, df, table_name, mode):
        """
        saves spark dataframe into postgresql
        """
        save_time_start = time.time()
        df.write.format('jdbc')\
                .option('url', self.credent['psql']["url"])\
                .option('driver', self.credent['psql']["driver"])\
                .option('user', self.credent['psql']['user'])\
                .option('password', self.credent['psql']['passwd'])\
                .option('dbtable', table_name)\
                .mode(mode)\
                .save()
        

    def spark_transform(self):
        """
        performs data sessionization on tracking data
        labels trips with their departure and destination port names
        """
        self.ship_info_df = self.df.select("MMSI", "VesselName", "IMO","Length",
                                           "Width", "Draft", "Cargo").distinct()
        self.df_with_label = tools.group_trips(self.df)
        self.trips_df = tools.gen_trip_table(self.df_with_label)
        self.trips_df = label.label_ports(self.trips_df, 0.05)
        self.trip_log_df = self.df_with_label.select("MMSI", "BaseDateTime",
                                                     "LAT", "LON")
        self.port_heat_df = label.port_heat_map(self.trip_log_df, 0.1)


    def run(self):
        """
        execute the data pipeline
        """
        #psql.create_tables(self.schema_conf, self.credent)
        self.read_from_s3()
        self.spark_transform()
        #self.save_to_psql(self.ship_info_df, 'ship_info', 'append')
        #self.save_to_psql(self.trips_df, 'trips', 'append')
        #self.save_to_psql(self.trip_log_df, 'trip_log', 'append')
        #self.save_to_psql(self.port_heat_df, 'port_heat', 'overwrite')
