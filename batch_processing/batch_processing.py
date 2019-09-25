import pyspark
from pyspark.sql import SparkSession
import sys
sys.path.append('../tools/')
import tools as t
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, StringType
import yaml


class BatchTransformer:
    """
    class that reads data from S3 bucket,
    process it by using spark
    and save results into postgresql database
    """

    def __init__(self, s3_configfile, psql_configfile, schema_configfile, credentfile):
        self.spark = SparkSession.builder.getOrCreate()
        self.credent = yaml.load(open(credentfile, 'r'))
        self.s3_conf = yaml.load(open(s3_configfile, 'r'))
        self.psql_conf = yaml.load(open(psql_configfile, 'r'))
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


    def save_to_psql(self, df):
        """
        save spark dataframe into postgresql though jdbc driver
        """
        df.write.format('jdbc')\
                .option('url', self.credent['psql']["url"])\
                .option('driver', self.credent['psql']["driver"])\
                .option('user', self.credent['psql']['user'])\
                .option('password', self.credent['psql']['passwd'])\
                .option('dbtable', self.psql_conf['dbtable'])\
                .mode(self.psql_conf['mode'])\
                .save()


    def spark_transform(self):
        df_with_label = t.label_trip(self.df)
        trip_df = t.gen_trip_table(df_with_label)
        trip_df.show(100)


    def run(self):
        """
        execute the data pipeline
        """
        self.read_from_s3()
        self.spark_transform()


def main():
    s3_configfile = '../config/s3_config.yaml'
    schema_configfile = '../config/trip_schema.yaml'
    psql_configfile = '../config/psql_config.yaml'
    credentfile = '../config/credentials.yaml'
    trips = BatchTransformer(s3_configfile, psql_configfile, schema_configfile, credentfile)
    trips.run()

if __name__ == '__main__':
    main()
