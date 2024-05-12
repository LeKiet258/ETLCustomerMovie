from common.SparkSessionSingleton import SparkSessionSingleton

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import time

PG_PROPERTIES = {
    "user": "postgres",
    "password": "1",
    "driver": "org.postgresql.Driver"
}


def save_df_to_pg(df: DataFrame, db_name: str, table_name: str) -> bool:
    method_name = "save_df_to_pg"
    postgres_url = f"jdbc:postgresql://localhost:5432/{db_name}"
    
    try: 
        df.write.jdbc(url=postgres_url, table=table_name, mode="overwrite", properties=PG_PROPERTIES)
        return True

    except Exception as e:
        print(f"{method_name} - Error when saving df to database. Details: {e}")
        return False
