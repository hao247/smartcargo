import unittest
import os
import sys
sys.path.append("../tools/")
import tools
import label_bucketizer as lb
from pyspark.sql.types import(
    StructType,
    StructField,
    TimestampType,
    IntegerType,
    DoubleType,
    StringType,
    LongType,
)
from pyspark.sql import SparkSession

#bin_localtion(df, lat, lon, rang, input_cols)
#label_ports(df, rang)
#port_heat_map(df, rang)

class TestMethods(unittest.TestCase):
    """
    class of unit tests for methods in tools.py and label_bucketizer.py
    """


    def setUp(self):
        """
        initialize test suites
        """
        pass


    def tearDown(self):
        """
        cleanup test suites
        """
        pass


    def test_group_trips(self):
        # test on group_trips
        spark = SparkSession.builder.getOrCreate()
        data = [(1, '2017-01-04 01:00:00', 52.58473, -174.02316, 10.0)]
        schema = ['MMSI', 'BaseDateTime', 'LAT', 'LON', 'SOG']
        return_schema = StructType(
                            [
                                StructField('MMSI', LongType(), True),
                                StructField('BaseDateTime', StringType(), True),
                                StructField('LAT', DoubleType(), True),
                                StructField('LON', DoubleType(), True),
                                StructField('TripID', LongType(), True),
                            ]
                        )
        df = spark.createDataFrame(data, schema)
        return_df = tools.group_trips(df)
        self.assertEqual(return_schema,
                         return_df.schema,
                         'Bad schema from group_trips')


    def test_gen_trip_table(self):
        # test on gen_trip_table
        spark = SparkSession.builder.getOrCreate()
        data = [(1, '2017-01-04 01:00:00', 52.58473, -174.02316, 10.0)]
        schema = ['MMSI', 'BaseDateTime', 'LAT', 'LON', 'SOG']
        return_schema = StructType(
                            [
                                StructField('TripID', LongType(), True),
                                StructField('MMSI', LongType(), True),
                                StructField('TIME_START', StringType(), True),
                                StructField('TIME_END', StringType(), True),
                                StructField('LAT_START', DoubleType(), True),
                                StructField('LON_START', DoubleType(), True),
                                StructField('LAT_END', DoubleType(), True),
                                StructField('LON_END', DoubleType(), True),
                            ]
                        )
        df = spark.createDataFrame(data, schema)
        df = tools.group_trips(df)
        return_df = tools.gen_trip_table(df)
        self.assertEqual(return_schema,
                         return_df.schema,
                         'Fail to generate trip_table')


    def test_label_ports(self):
        #test on label_ports
        spark = SparkSession.builder.getOrCreate()
        data = [(123, 1, 42.342676, -71.019450, 40.659301, -74.078841)]
        schema = ['TripID', 'MMSI', 'LAT_START', 'LON_START', 'LAT_END', 'LON_END']
        expect_ports = ['Boston, MA', 'New York, NY and NJ']
        df = spark.createDataFrame(data, schema)
        return_df = lb.label_ports(df, 0.05).collect()
        return_ports = [return_df[0][2], return_df[0][3]]
        self.assertEqual(expect_ports,
                         return_ports,
                         'Incorrect ports')


if __name__  == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
