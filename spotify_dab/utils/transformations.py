from databricks.sdk.runtime import *
from pyspark.sql import functions as F

class reusable:
    def __init__(self):
        self._sqldf = spark.sql("SELECT current_catalog() AS current_catalog")
        
    def dropColumns(self,df,columns):
        df=df.drop(*columns)
        return df
    def add_silver_audit_cols(df):
        return df.withColumn("silver_ingest_timestamp", F.current_timestamp()) \
                .withColumnRenamed("ingest_timestamp", "bronze_ingest_timestamp")
