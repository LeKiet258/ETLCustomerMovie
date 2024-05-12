import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


class SparkSessionSingleton:
    _instance = None
    
    @staticmethod
    def get_spark_session():
        if SparkSessionSingleton._instance is None:
            print('Initializing Spark session...')
            SparkSessionSingleton()
            
        return SparkSessionSingleton._instance
        
    def __init__(self):
        """Virtually private constructor"""
        if SparkSessionSingleton._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            sc = SparkContext() # dòng này phải nằm trước dòng dưới
            spark = SparkSession.builder \
                .config("spark.driver.memory", "15g") \
                .config("spark.network.timeout", "10000000") \
                .config("spark.executor.heartbeatInterval", "10000000") \
                .getOrCreate()
                
            SparkSessionSingleton._instance = spark
