import findspark
findspark.init('C:\SPARK')
from pyspark.sql import SparkSession
import json
import configparser
class MySpark:
    def __init__(self, config_path):
        self.config_path = config_path
        self.load_config()
        self.init_spark_session()

    def load_config(self):
        config = configparser.ConfigParser()
        config.read(self.config_path)
        self.config = dict(config['spark'])

    def init_spark_session(self):
        spark_builder = SparkSession.builder.appName(self.config.get('app_name', 'DefaultAppName'))

        for key, value in self.config.get('spark_configs', {}).items():
            spark_builder.config(key, value)
        # spark_builder.config("spark.jars", "ojdbc8.jar")
        # spark_builder.config("spark.driver.extraClassPath", "ojdbc8.jar")
        # spark_builder.config("spark.executor.extraClassPath", "ojdbc8.jar")

        self.spark = spark_builder.getOrCreate()


    def get_spark_session(self):
        self.spark.catalog.clearCache()
        return self.spark
