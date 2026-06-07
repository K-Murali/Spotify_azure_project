import dlt
import pyspark.sql.functions as F


# ═══════════════════════════════════════════════════════════════════════════
# DATA QUALITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def apply_dq_dimartist(df):
    return df \
        .withColumn("dq_fail_reason",
            F.when(F.col("artist_id").isNull(),   "artist_id is null")
             .when(F.col("artist_name").isNull(), "artist_name is null")
             .when(F.col("genre").isNull(), "genre is null")
             .when(F.col("country").isNull(), "country is null")
             .otherwise(None)
        )\
        .withColumn("dq_is_valid", F.col("dq_fail_reason").isNull())


# ═══════════════════════════════════════════════════════════════════════════
# DIM ARTIST (SCD1)
# ═══════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="dimartist_stg",
    comment="DimArtist staging"
)
@dlt.expect_or_drop("valid_artist_name", "LENGTH(artist_name) >= 3")
def dimartist_stg():
    df = spark.readStream.table("spotify_catalog.bronze.dimartist") \
        .withColumn("artist_sk", F.hash(F.col("artist_id")))\
        .withColumn("silver_ingest_timestamp", F.current_timestamp()) \
        .withColumnRenamed("ingest_timestamp", "bronze_ingest_timestamp")
    return apply_dq_dimartist(df)


dlt.create_streaming_table(
    name="dimartist",
    comment="DimArtist SCD1 Silver"
)

dlt.create_auto_cdc_flow(
    target="dimartist",
    source="dimartist_stg",
    keys=["artist_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1,
    track_history_except_column_list=None,
    name=None,
    once=False
)