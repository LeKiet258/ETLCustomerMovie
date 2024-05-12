import os
from functools import reduce
from datetime import datetime, timedelta
import time

from common.SparkSessionSingleton import SparkSessionSingleton
from pyspark.sql.types import *
from pyspark.sql.functions import *

def read_data(path_json_data: str):
    """read data from a single json file path 

    Args:
        path_json_data (str): relative or absolute path

    Returns:
        DataFrame
    """
    print("---------------Start reading data---------------")
    spark = SparkSessionSingleton.get_spark_session()
    
    file_name = os.path.basename(path_json_data)
    log_date = file_name.split('.')[0]
    df_input = spark.read.json(path_json_data).withColumn("Date", to_date(lit(log_date), "yyyyMMdd"))
    
    # df_input.show()
    
    return df_input

def fetch_data_files_between(from_date_str: str, to_date_str: str, skip=True, parent_folder = 'E:/Dataset/log_content') -> list:
    """
    fetch all file paths in the given date range inclusively.

    Args:
        from_date_str, to_date_str (str): date format 'yyyy-MM-dd'
        skip=True (boolean): if true, skip the nonexistent file path, otherwise raise exception

    Returns:
        list<string>: the list of files (absolute path) in the given date range
    """
    from_date = datetime.strptime(from_date_str, "%Y-%m-%d")
    to_date = datetime.strptime(to_date_str, "%Y-%m-%d")
    list_json_files = []
    
    n = (to_date - from_date).days + 1
    for i in range(n):
        next_date = from_date + timedelta(days = i)
        next_date_str = next_date.strftime("%Y%m%d")
        file_path = f"{parent_folder}/{next_date_str}.json"
        
        if skip:
            list_json_files.append(file_path)
        
        if not skip and not os.path.exists(file_path):
            raise Exception(f"{file_path} does not exist")

    return list_json_files

def read_data_by_date(from_date_str: str, to_date_str: str) -> DataFrame:
    """read json data from the given date range inclusively

    Args:
        from_date_str (str), to_date_str (str): date format 'yyyy-MM-dd'

    Returns:
        DataFrame
    """
    method_name = 'read_data_by_date'
    spark = SparkSessionSingleton.get_spark_session()
    list_json_files = fetch_data_files_between(from_date_str, to_date_str)
    
    print(f"{method_name} - No. files: {len(list_json_files)}")
    
    # This pattern matches the last occurrence of a forward slash (/) followed by one or more characters that are not a forward slash (the file name)
    df_input = spark.read.json(list_json_files) \
        .withColumn('FileName', regexp_extract(input_file_name(), r"/([^/]+)\.json$", 1)) \
        .withColumn('Date', to_date(col('FileName'), 'yyyyMMdd'))
    
    return df_input

