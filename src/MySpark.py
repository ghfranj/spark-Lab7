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

        self.spark = spark_builder.getOrCreate()

    def get_spark_session(self):
        return self.spark
