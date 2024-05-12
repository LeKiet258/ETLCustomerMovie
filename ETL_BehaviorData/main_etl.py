import findspark
findspark.init()

import os
from functools import reduce
from datetime import datetime, timedelta
import time

from ..common.SparkSessionSingleton import SparkSessionSingleton
from common import ReadUtil, WriteUtil
from pyspark.sql.types import *
from pyspark.sql.functions import *

# GLOBALS
spark = SparkSessionSingleton.get_spark_session()

# FUNCTIONS
def preprocess_data(df):
    print("---------------Preprocessing data---------------")
    
    df_source = df.select('Date', '_source.Contract', '_source.AppName', '_source.TotalDuration', '_source.Mac')
    return df_source
    
def categorize_app(df):
    print("---------------Catgorizing data AppName---------------")
    
    # rule
    mapping = {
        'CHANNEL':'TVDuration',
        'DSHD':'TVDuration', 
        'KPLUS':'TVDuration',
        'VOD' : 'MovieDuration', 
        'FIMS_RES': 'MovieDuration', 
        'BHD_RES': 'MovieDuration', 
        'BHD': 'MovieDuration', 
        'DANET': 'MovieDuration', 
        'VOD_RES': 'MovieDuration', 
        'FIMS': 'MovieDuration',
        'SPORT': 'SportDuration', 
        'RELAX': 'RelaxDuration',
        'CHILD': 'ChildDuration'  
    }

    df_category = df.rdd \
        .map(lambda row: (row.Date, row.Contract, mapping.get(row.AppName, 'OthersDuration'), row.Mac, row.TotalDuration)) \
        .toDF(["Date", "Contract", "Category", "Mac", "TotalDuration"])
        
    return df_category

def compute_active_days(df: DataFrame) -> DataFrame:
    df_active_days_by_contract = df.groupBy('Contract').agg(countDistinct("Date").alias("Activeness"))
    return df_active_days_by_contract

def compute_metrics(df: DataFrame) -> DataFrame:
    print('---------------compute MostWatched, CustomerTaste & Activeness columns---------------')
    date = df.select(date_format(col('Date'), 'yyyyMMdd').alias('Date')).first()['Date']
    
    # create unique temp view when processing in parallel
    view_name = f'source_{date}'
    df.createOrReplaceTempView(view_name) # cols: ["Date", "Contract", "Category", "Mac", "TotalDuration"]
    
    df_metrics = spark.sql(f"""
        WITH cte_most_watch as (
            select Contract, regexp_replace(Category, 'Duration', '') as MostWatch
            from (
                select *, 
                    rank() over (partition by Contract order by TotalDuration desc) as rnk
                from source_{date}
            ) t
            where rnk = 1
        ),
        cte_customer_taste as (
            select Contract, 
                collect_set(regexp_replace(Category, 'Duration', '')) as CustomerTaste,
                count(distinct(Date)) as Activeness
            from source_{date}
            group by Contract
        )
        
        select w.Contract, MostWatch, CustomerTaste, t.Activeness
        from cte_most_watch w FULL OUTER JOIN cte_customer_taste t on w.Contract = t.Contract
    """)
    
    spark.catalog.dropTempView(view_name)
    return df_metrics
    

def compute_metrics_v2(df_pivot: DataFrame):
    print('---------------compute MostWatched, CustomerTaste columns V2---------------')
    list_duration_columns = ["ChildDuration", "MovieDuration", "RelaxDuration", "SportDuration", "TVDuration"]
    
    max_duration_value = greatest(*[col(col_name) for col_name in list_duration_columns])
    customer_taste_conditions = [when(col(col_name).isNotNull(), col_name.replace("Duration", "")) for col_name in list_duration_columns]
    
    df_metrics = df_pivot.withColumn(
        "MostWatch",
        when(col("ChildDuration") == max_duration_value, "Child")
        .when(col("MovieDuration") == max_duration_value, "Movie")
        .when(col("RelaxDuration") == max_duration_value, "Relax")
        .when(col("SportDuration") == max_duration_value, "Sport")
        .when(col("TVDuration") == max_duration_value, "TV"),
    ) \
    .withColumn("CustomerTaste", concat_ws("_", *customer_taste_conditions))
    
    return df_metrics

def sum_duration_by_category(df):
    print("---------------Summing duration by category---------------")
    
    df_duration_by_category = df \
        .groupBy("Contract", "Category") \
        .agg(sum("TotalDuration").alias("TotalDuration")) 
        
    df_pivot = df_duration_by_category.groupBy("Contract") \
        .pivot("Category") \
        .agg(sum("TotalDuration").alias("TotalDuration"))
        
    return df_pivot


def etl_one_day(file_path) -> DataFrame:
    print("---------------Start ETL one day---------------")
    
    df_input = ReadUtil.read_data(file_path)

    t1 = time.time()
    df_preprocess = preprocess_data(df_input)
    t_preprocess = time.time() - t1
    
    t1 = time.time()
    df_appname_category = categorize_app(df_preprocess)
    t_categorize = time.time() - t1 
    
    t1 = time.time()
    df_pivot_appname_duration = sum_duration_by_category(df_appname_category)
    t_sum_duration_by_category = time.time() - t1
    
    # compute metrics: MostWatch, CustomerTaste,activeness
    t1 = time.time()
    df_metrics = compute_metrics(df_appname_category)
    t_compute_metrics = time.time() - t1
    
    t1 = time.time()
    df_one_day_final = df_pivot_appname_duration.join(df_metrics, ['Contract'])
    t_prep_df_final = time.time() - t1
    
    # compute metric: MostWatch & CustomerTaste
    df_metrics = compute_metrics(df_appname_category)
    
    df_pivot_appname_duration = sum_duration_by_category(df_appname_category)
    df_one_day_final = df_pivot_appname_duration.join(df_metrics, ['Contract'], 'outer')
    
    return df_one_day_final

def process_data_2_days(df1: DataFrame, df2: DataFrame) -> DataFrame:
    print("---------------Merge and process df of 2 days---------------")
    df_merge = df1.unionByName(df2, allowMissingColumns=True)
    
    df_processed = df_merge.groupBy("Contract") \
        .agg(
            sum('TVDuration').alias('TVDuration'),
            sum('MovieDuration').alias('MovieDuration'),
            sum('ChildDuration').alias('ChildDuration'),
            sum('RelaxDuration').alias('RelaxDuration'),
            sum('SportDuration').alias('SportDuration'),
        )
        
    return df_processed
    

def etl_one_month(folder_month_path):
    log_files = [folder_month_path + file_name for file_name in os.listdir(folder_month_path) if file_name.endswith(".json")]
    
    list_df_30_days = list(map(etl_one_day, log_files))
    
    df_final = functools.reduce(process_data_2_days, list_df_30_days)
    return df_final

def etl_date_range_v2(from_date: str, to_date: str) -> DataFrame:
    '''
    read and transform for each file and concat into a single df. Finally, do final tranformation on the single df
    '''
    log_files = ReadUtil.fetch_data_files_between(from_date, to_date)
    df_appname_all_time = None
    
    for log_file in log_files:
        df_input = ReadUtil.read_data(log_file)

        t1 = time.time()
        df_preprocess = preprocess_data(df_input)
        t_preprocess = time.time() - t1
        
        t1 = time.time()
        df_appname_category = categorize_app(df_preprocess)
        t_categorize = time.time() - t1 
        
        # t1 = time.time()
        # df_pivot_appname_duration = sum_duration_by_category(df_appname_category)
        # t_sum_duration_by_category = time.time() - t1
        
        # # compute metrics: MostWatch, CustomerTaste,activeness
        # t1 = time.time()
        # df_metrics = compute_metrics(df_appname_category)
        # t_compute_metrics = time.time() - t1

        
        if df_appname_all_time is None:
            df_appname_all_time = df_appname_category
        else:
            df_appname_all_time = df_appname_all_time.unionByName(df_appname_category, allowMissingColumns=True)
            
    # tham khảo bài class 5 - tiến thành
    df_metrics = compute_metrics(df_appname_all_time)
    df_pivot_appname_duration = sum_duration_by_category(df_appname_all_time)
    
    df_final = df_pivot_appname_duration.join(df_metrics, ['Contract'])
    return df_final
    

def etl_date_range_v1(from_date: str, to_date: str) -> DataFrame:
    """
    read all files at once, then transform one single df
    """
    method_name = 'etl_date_range'
    print(f"---------------etl_date_range: read once, process once---------------")
    t0 = time.time()
    
    t1 = time.time()
    df_input = ReadUtil.read_data_by_date(from_date, to_date)
    t_read_files = time.time() - t1
    
    t1 = time.time()
    df_preprocess = preprocess_data(df_input)
    df_preprocess.persist()
    t_preprocess = time.time() - t1
    
    # TODO: persist & compute active days 
    t1 = time.time()
    df_activeness = compute_active_days(df_preprocess)
    t_compute_activeness = time.time() - t1 

    t1 = time.time()
    df_appname_category = categorize_app(df_preprocess)
    t_categorize = time.time() - t1 
    
    t1 = time.time()
    df_pivot_appname_duration = sum_duration_by_category(df_appname_category)
    df_pivot_appname_duration.persist() # mem & disk by default
    t_sum_duration_by_category = time.time() - t1
    
    # compute metrics: MostWatch, CustomerTaste,activeness
    t1 = time.time()
    
    df_metrics = compute_metrics_v2(df_pivot_appname_duration)
    # df_metrics = compute_metrics(df_appname_category)
    t_compute_metrics = time.time() - t1
    
    t1 = time.time()
    df_final = df_metrics.join(df_activeness, ['Contract'])
    # df_final = df_pivot_appname_duration.join(df_metrics, ['Contract'])
    t_prep_df_final = time.time() - t1
    
    t1 = time.time()
    WriteUtil.save_df_to_pg(df_final, 'movie', 'contract_duration')
    t_save_db = time.time() - t1
    
    print(f"""{method_name} - Execute time: {time.time() - t0}
          , t_read_files: {t_read_files}
          , t_preprocess: {t_preprocess}
          , t_categorize: {t_categorize}
          , t_sum_duration_by_category: {t_sum_duration_by_category}
          , t_compute_metrics: {t_compute_metrics}
          , t_compute_activeness: {t_compute_activeness}
          , t_prep_df_final: {t_prep_df_final}
          , t_save_db: {t_save_db}"""
    )
    
    print("DEBUG df_final:")
    # df_final.show(5)
    
    if df_preprocess.is_cached:
        print(f'{method_name} - Unpersist df_input')
        df_preprocess.unpersist()
        
    if df_pivot_appname_duration.is_cached:
        print(f'{method_name} - Unpersist df_pivot_appname_duration')
        df_pivot_appname_duration.unpersist()

    return df_final

    

if __name__ == '__main__':
    try:
        df = etl_date_range_v1('2022-04-01', '2022-04-02')
        # df.persist()
        
        # print(f"no. rows: {df.count()}")
        # df.show(5)
        
        # df.unpersist()
        
        # df = etl_one_month("./one_month/")
        # df.show()
        
        # df = etl_one_day('.\\20220401.json')
        
    except Exception as e:
        raise e
    
    finally:
        # spark.stop()
        print("End task")