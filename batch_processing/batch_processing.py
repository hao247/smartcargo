import pyspark
from pyspark.sql import SparkSession
import sys
import json
import psycopg2


class BatchTransformer:
    """
    class that reads data from S3 bucket,
    process it by using spark
    and save results into postgresql database
    """

    def __init__(self, s3_configfile, raw_data_fields_configfile, psql_configfile, schema_configfile, credentfile):
        self.spark = SparkSession.builder.getOrCreate()
        self.credent = json.load(open(credentfile, 'r'))
        self.s3_conf = json.load(open(s3_configfile, 'r'))
        self.raw_data_fields_conf = json.load(open(raw_data_fields_configfile, 'r'))
        self.psql_conf = json.load(open(psql_configfile, 'r'))
        self.schema_conf = json.load(open(schema_configfile, 'r'))



    def read_from_s3(self):
    """
    read files from s3 bucket and create spark dataframe
    """
        s3file = 's3a://{}/{}/{}'.format(self.s3_conf['BUCKET'], self.s3_conf['FOLDER'], self.s3_conf['FILE'])
        
        self.data = self.spark.read\
                .format('csv')\
                .option('header', True)\
                .load(s3file)\
                .select(list(self.raw_data_fields_conf.keys()))


    def save_to_psql(self):
    """
    save spark dataframe into postgresql though jdbc driver
    """
        self.data.write.format('jdbc')\
                .mode(self.psql_conf["mode"])\
                .option('url', self.psql_conf["url"])\
                .option('driver', self.psql_conf["driver"])\
                .option('user', self.credent["psql_user"])\
                .option('password', self.credent["psql_passwd"])\
                .option('dbtable', self.psql_conf[dbtable])\
                .save()


    def save_to_psql_2(self):
    """
    another way to save spark dataframe into postgresql by psycopg2 module
    """
        conn = psycopg2.connect(host=self.psql_conf["host"], database=self.psql_conf["dbname"], user=self.credent["psql_user"], password=self.credent["psql_passwd"])
        cursor = conn.cursor()

        keys = self.schema_conf.keys()
        fields = ','.join(i for i in keys)
        for row in self.data.collect():
            values = ','.join("\'%s\'"%(row[i]) for i in keys)
            cursor.execute('insert into trips (' + fields +') values(' + values + ')')

        conn.commit()
        cursor.close()
        conn.close()


    def spark_transform(self):
        self.data = self.data\
                .filter(self.data.MMSI == '564294000')\
                .orderBy(self.data.BaseDateTime)
        self.data.show()


    def run(self):
    """
    execute the data pipeline
    """
        self.read_from_s3()
        self.spark_transform()
        self.save_to_psql_2()


def main():
    s3_configfile = '../config/s3bucket.config'
    raw_data_fields_configfile = '../config/raw_data_fields.config'
    schema_configfile = '../config/trip_schema.config'
    psql_configfile = '../config/psql.config'
    credentfile = '../config/credentials.txt'
    trips = BatchTransformer(s3_configfile, raw_data_fields_configfile, psql_configfile, schema_configfile, credentfile)
    trips.run()

if __name__ == '__main__':
    main()
