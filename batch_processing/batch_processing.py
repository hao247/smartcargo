import pyspark
from pyspark.sql import SparkSession
import sys
sys.path.append('../tools/')
import tools as t
import yaml
import psycopg2


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
        
        self.df = self.spark.read\
                .format('csv')\
                .option('header', True)\
                .option('inferSchema', True)\
                .load(s3file)


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


    def save_to_psql_2(self, df):
        """
        another way to save spark dataframe into postgresql by psycopg2 module
        """
        conn = psycopg2.connect(host=self.psql_conf["host"], database=self.psql_conf["dbname"], user=self.credent["psql_user"], password=self.credent["psql_passwd"])
        cursor = conn.cursor()

        keys = self.schema_conf.keys()
        fields = ','.join(i for i in keys)
        for row in self.df.collect():
            values = ','.join("\'%s\'"%(row[i]) for i in keys)
            cursor.execute('insert into trips (' + fields +') values(' + values + ')')

        conn.commit()
        cursor.close()
        conn.close()


    def spark_transform(self):
        ship_list = t.list_mmsi(self.df)
        print(ship_list)
        for ship_mmsi in ship_list:
            print('ship_mmsi: ', ship_mmsi)
            df_one_ship = t.label_trip(self.df, ship_mmsi)
            self.save_to_psql(df_one_ship)


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
