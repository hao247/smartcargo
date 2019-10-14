from pyspark.sql import SparkSession
import sys
sys.path.append('../tools/')
import tools
import postgres as psql
import label_bucketizer as label
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, StringType
import yaml
import time
#import psycopg2


class BatchTransformer:
    """
    class that reads data from S3 bucket,
    process it by using spark
    and save results into postgresql database
    """

    def __init__(self, s3_configfile, schema_configfile, credentfile):
        self.spark = SparkSession.builder.getOrCreate()
        self.credent = yaml.load(open(credentfile, 'r'))
        self.s3_conf = yaml.load(open(s3_configfile, 'r'))
        self.schema_conf = yaml.load(open(schema_configfile, 'r'))


    def read_from_s3(self):
        """
        read files from s3 bucket and create spark dataframe
        """
        s3file = 's3a://{}/{}/{}'.format(self.s3_conf['bucket'], self.s3_conf['folder'], self.s3_conf['file'])
        
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
        


    #def create_tables(self):
    #    """
    #    create three tables and convert trip_log table into hypertable:
    #    ship_info:  specs of each ship
    #    trips:      trip's start and end position/time of trips
    #    trip_log:   details of all trips
    #    """
    #    conn = psycopg2.connect(\
    #            host=self.credent['psql']['host'],\
    #            database=self.credent['psql']['dbname'],\
    #            user=self.credent['psql']['user'],\
    #            password=self.credent['psql']['passwd']\
    #            )
    #    cursor = conn.cursor()

    #    for table_name in self.schema_conf.keys():
    #        fields = ','.join(field + " " + self.schema_conf[table_name]['fields'][field] for field in self.schema_conf[table_name]['order'])
    #        cursor.execute('create table if not exists {} ({})'.format(table_name, fields))
    #    
    #    conn.commit()
    #    cursor.execute("select create_hypertable('trip_log', 'basedatetime', if_not_exists => TRUE)")
    #    conn.commit()
    #    cursor.close()
    #    conn.close()

    #    
    #def drop_tables(self):
    #    conn = psycopg2.connect(host=self.credent['psql']['host'],\
    #            database=self.credent['psql']['dbname'],\
    #            user=self.credent['psql']['user'],\
    #            password=self.credent['psql']['passwd'])
    #    cursor = conn.cursor()
    #    cursor.execute("drop table if exists ship_info, trips")
    #    cursor.execute("drop table if exists trip_log")
    #    conn.commit()
    #    cursor.close()
    #    conn.close()


    def save_to_psql(self, df, table_name, mode):
        """
        save spark dataframe into postgresql though jdbc driver
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
       # self.ship_info_df = self.df.select("MMSI", "VesselName", "IMO", "Length", "Width", "Draft", "Cargo").distinct()
        self.df_with_label = tools.group_trips(self.df)
        #self.trips_df = tools.gen_trip_table(self.df_with_label)
        self.trip_log_df = self.df_with_label.select("MMSI", "BaseDateTime", "LAT", "LON")
        #self.trips_df_1 = label.label_ports(self.trips_df, 0.05)
        #self.trips_df_1.filter((self.trips_df_1.PORT_START != "null") & (self.trips_df_1.PORT_END != 'null')).show(100)
        self.port_heat_df = label.port_heat_map(self.trip_log_df, 0.1)


    def run(self):
        """
        execute the data pipeline
        """
        time_start = time.time()
        #psql.drop_tables(self.schema_conf, self.credent)
        #psql.create_tables(self.schema_conf, self.credent)
        self.read_from_s3()
        self.spark_transform()
        #self.save_to_psql(self.ship_info_df, 'ship_info', 'append')
        #self.save_to_psql(self.trips_df, 'trips', 'append')
        #self.save_to_psql(self.trip_log_df, 'trip_log', 'append')
        self.save_to_psql(self.port_heat_df, 'port_heat', 'overwrite')
        time_end = time.time()
        
        print("Time for ETL: ", time_end - time_start)

def main():
    s3_configfile = '../config/s3_config.yaml'
    schema_configfile = '../config/schema.yaml'
    credentfile = '../config/credentials.yaml'
    trips = BatchTransformer(s3_configfile, schema_configfile, credentfile)
    trips.run()

if __name__ == '__main__':
    main()
