import sys
# print('Original sys.path:', sys.path)
sys.path.append('d:\\DE\\study_de\\ETLCustomerMovie\\')

from common.SparkSessionSingleton import SparkSessionSingleton
from common import ReadUtil, WriteUtil

from pyspark.sql.types import *
from pyspark.sql.functions import *

from Levenshtein import distance

def read_log_search(month: int):
    spark = SparkSessionSingleton.get_spark_session()
    month_str = f'0{month}' if month < 10 else str(month)
    
    df = spark.read.parquet(f"*{month_str}*.parquet")
    return df

# Define UDF for Levenshtein distance. Range: [0,1], if 1 then exact match
def levenshtein_similarity(str1, str2):
    return 1 - (distance(str1, str2) / max(len(str1), len(str2)))

def find_most_search(top_n_users: int):
    spark = SparkSessionSingleton.get_spark_session()
    parent_folder = 'E:/Dataset/log_search'
    
    df_log_search = spark.read.parquet(f"{parent_folder}/*/*.parquet") \
        .withColumn('date', to_date(col('datetime')))
    
    df_category_mapping = spark.read.csv('temp/map_search_category.csv', header=True) \
        .withColumnRenamed('Column1', 'category') \
        .where('category is not null')
        
    df_log_search.createOrReplaceTempView('log_search')
    df_category_mapping.createOrReplaceTempView('category_mapping')
        
    sql = f"""
        WITH cte_text_frequency_t6 as (
            select user_id, keyword, count(1) as frequency
            from log_search
            where month(date) = 6 AND user_id is not null AND keyword is not null
            group by user_id, keyword
        ),
        cte_text_frequency_t7 as (
            select user_id, keyword, count(1) as frequency
            from log_search
            where month(date) = 7 AND user_id is not null AND keyword is not null
            group by user_id, keyword
        ),
        cte_most_search_t6 as (
            select user_id, keyword as most_search_t6 
            from (
                select user_id, keyword, 
                    row_number() over (partition by user_id order by frequency desc) as rnk
                from cte_text_frequency_t6
            ) 
            where rnk = 1
        ),
        cte_most_search_t7 as (
            select user_id, keyword as most_search_t7 
            from (
                select user_id, keyword, 
                    row_number() over (partition by user_id order by frequency desc) as rnk
                from cte_text_frequency_t7
            )
            where rnk = 1
        ),
        cte_most_search_t6_w_category as (
            select user_id, most_search_t6, category as category_t6
            from cte_most_search_t6 t6 join category_mapping cm on lower(most_search_t6) = lower(cm.text)
        ),
        cte_most_search_t7_w_category as (
            select user_id, most_search_t7, category as category_t7
            from cte_most_search_t7 t7 join category_mapping cm on lower(most_search_t7) = lower(cm.text)
        )

        
        -- main sql
        select t6.user_id, most_search_t6, most_search_t7, category_t6, category_t7,
            case when category_t6 = category_t7 then 'unchanged' else 'changed' end as trending_type,
            case when category_t6 = category_t7 then 'unchanged' else concat_ws('_', category_t6, category_t7) end as previous
        from cte_most_search_t6_w_category t6 join cte_most_search_t7_w_category t7 on t6.user_id = t7.user_id
        {f'limit {top_n_users}' if top_n_users > 0 else ''}
    """
    
    df_most_search_full = spark.sql(sql)
    
    # df_most_search_full.coalesce(1).write.parquet('temp/most_search_t67.parquet')
    spark.catalog.dropTempView("log_search")
    spark.catalog.dropTempView("category_mapping")
    
    
    return df_most_search_full
    

    
    
df = find_most_search(1000)
WriteUtil.save_df_to_pg(df, "movie", "tracking_log_search")
df.show()