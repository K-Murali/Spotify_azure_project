import dlt
import pyspark.sql.functions as F

@dlt.table
def dimtrack_stg():
    df = spark.readStream.table("spotify_catalog.bronze.dimtrack") \
        .withColumn("track_sk", F.col("track_id")) \
        .withColumn("silver_ingest_timestamp", F.current_timestamp()) \
        .withColumnRenamed("ingest_timestamp", "bronze_ingest_timestamp")
    return df


dlt.create_streaming_table("dimtrack")

dlt.create_auto_cdc_flow(
    target="dimtrack",
    source="dimtrack_stg",
    keys=["track_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1,
    track_history_except_column_list=None,
    name=None,
    once=False
)