"""
Spotify Silver Layer Pipeline
Bronze → Silver with DQM, Dedup, SCD1/SCD2
"""

from pyspark.sql import functions as F, Window
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

SILVER_BASE = "abfss://deltalake@mkazureproject1.dfs.core.windows.net/silver"

# Use timestamped checkpoint to avoid conflicts
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
CHECKPOINT_BASE = f"{SILVER_BASE}/checkpoints/{RUN_ID}"


# ═══════════════════════════════════════════════════════════════════════════
# DQM FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def apply_dq_dimuser(df):
    return df \
        .withColumn("dq_is_valid",
            (F.col("user_id").isNotNull()) &
            (F.col("user_name").isNotNull()) &
            (F.col("country").isNotNull())&
            (F.col("subscription_type").isNotNull())
        ) \
        .withColumn("dq_fail_reason",
            F.when(F.col("user_id").isNull(),   "Missing user_id")
             .when(F.col("user_name").isNull(), "Missing user_name")
              .when(F.length(F.col("user_name")) <= 2, "Invalid user_name")
             .when(F.col("country").isNull(),   "Missing country")
             .when(F.col("subscription_type").isNull(), "Missing subscription_type")
             .when(F.col("subscription_type").isin(["Premium", "Free", "Family"]) == False,"Invalid subscription_type")
             .otherwise(None)
        )

def apply_dq_factstream(df):
    return (
        df
        .withColumn(
            "dq_is_valid",
            (F.col("stream_id").isNotNull()) &
            (F.col("user_id").isNotNull()) &
            (F.col("track_id").isNotNull()) &
            (F.col("stream_timestamp").isNotNull()) &
            (F.col("device_type").isNotNull()) &
            (F.col("listen_duration").isNotNull()) &
            (F.col("listen_duration") > 0)
        )
        .withColumn(
            "dq_fail_reason",
            F.when(F.col("stream_id").isNull(), "Missing stream_id")
             .when(F.col("user_id").isNull(), "Missing user_id")
             .when(F.col("track_id").isNull(), "Missing track_id")
             .when(F.col("stream_timestamp").isNull(), "Missing stream_timestamp")
             .when(F.col("device_type").isNull(), "Missing device_type")
             .when(F.col("listen_duration").isNull(), "Missing listen_duration")
             .when(F.col("listen_duration") <= 0, "Invalid listen_duration")
             .otherwise(None)
        )
    )

# ═══════════════════════════════════════════════════════════════════════════
# QUARANTINE
# ═══════════════════════════════════════════════════════════════════════════

def write_quarantine(df, table_name, pk_col):
    df.select(
        F.lit(table_name).alias("source_table"),
        F.col(pk_col).cast("string").alias("natural_key"),
        F.to_json(F.struct("*")).alias("raw_record"),
        "dq_fail_reason",
        F.current_timestamp().alias("quarantine_timestamp"),
        F.col("captured_file_path").alias("input_file_path")
    ).write.format("delta").mode("append").saveAsTable(
        "spotify_catalog.silver.dqm_log"
    )


# ═══════════════════════════════════════════════════════════════════════════
# SCD2: DimUser
# ═══════════════════════════════════════════════════════════════════════════

def pipeline_dqm_dedup_scd2_for_dimuser(batch_df, batch_id):
    print(f"[DimUser SCD2] Processing batch {batch_id}...")

    batch_df       = batch_df.withColumn("captured_file_path", F.col("input_file_path"))
    dqm_applied_df = apply_dq_dimuser(batch_df)

    valid_df   = dqm_applied_df.filter(F.col("dq_is_valid") == True)
    invalid_df = dqm_applied_df.filter(F.col("dq_is_valid") == False)

    invalid_count = invalid_df.count()
    valid_count   = valid_df.count()
    print(f"  Valid: {valid_count}, Invalid: {invalid_count}")

    if invalid_count > 0:
        write_quarantine(invalid_df, "DimUser", "user_id")
        print(f"  ✓ Quarantined {invalid_count} rows")

    if valid_count == 0:
        print("  ✓ No valid data")
        return

    deduped_df = valid_df.withColumn(
        "rn", F.row_number().over(
            Window.partitionBy("user_id").orderBy(F.col("updated_at").desc())
        )
    ).filter(F.col("rn") == 1).drop("rn")

    try:
        max_sk = spark.sql(
            "SELECT COALESCE(MAX(user_sk), 0) AS max_sk FROM spotify_catalog.silver.dimuser"
        ).collect()[0]["max_sk"]
    except Exception as e:
        print(f"  Warning: Could not fetch max_sk: {e}")
        max_sk = 0

    with_audit = deduped_df \
        .withColumn("silver_ingest_timestamp", F.current_timestamp()) \
        .withColumn("bronze_ingest_timestamp", F.col("ingest_timestamp")) \
        .withColumn("input_file_path",         F.col("captured_file_path")) \
        .withColumn("user_sk", F.row_number().over(Window.orderBy("user_id")) + F.lit(max_sk))

    print(f"  ✓ Generated user_sk from {max_sk + 1}")
    with_audit = with_audit.drop("dq_fail_reason")
    with_audit.createOrReplaceTempView("int_silver_dimuser")

    # Step 1: Close existing current records where attributes have changed
    spark.sql("""
        MERGE INTO spotify_catalog.silver.dimuser t
        USING int_silver_dimuser s
        ON  t.user_id = s.user_id
        AND t.scd_end_date IS NULL
        AND (   t.user_name         <> s.user_name
             OR t.country           <> s.country
             OR t.subscription_type <> s.subscription_type)
        WHEN MATCHED THEN UPDATE SET
            t.scd_end_date =s.start_date,
            t.is_current   = FALSE
    """)

    # Step 2: Insert new versions + brand-new users
    spark.sql("""
        MERGE INTO spotify_catalog.silver.dimuser t
        USING int_silver_dimuser s
        ON t.user_id = s.user_id AND t.scd_end_date IS NULL
        WHEN NOT MATCHED THEN INSERT (
            user_sk, user_id, user_name, country, subscription_type,
            scd_start_date, scd_end_date, is_current, updated_at,
            dq_is_valid,silver_ingest_timestamp, bronze_ingest_timestamp, input_file_path
        ) VALUES (
            s.user_sk, s.user_id, s.user_name, s.country, s.subscription_type,
            s.start_date, NULL, TRUE, s.updated_at,
            s.dq_is_valid,s.silver_ingest_timestamp, s.bronze_ingest_timestamp, s.input_file_path
        )
    """)

    print(f"✅ [DimUser SCD2] Batch {batch_id} complete\n")


# ═══════════════════════════════════════════════════════════════════════════
# SCD1: FactStream
# ═══════════════════════════════════════════════════════════════════════════

def pipeline_dqm_dedup_scd1_for_factstream(batch_df, batch_id):
    print(f"[FactStream SCD1] Processing batch {batch_id}...")

    batch_df       = batch_df.withColumn("captured_file_path", F.col("input_file_path"))
    dqm_applied_df = apply_dq_factstream(batch_df)

    valid_df   = dqm_applied_df.filter(F.col("dq_is_valid") == True)
    invalid_df = dqm_applied_df.filter(F.col("dq_is_valid") == False)

    invalid_count = invalid_df.count()
    valid_count   = valid_df.count()
    print(f"  Valid: {valid_count}, Invalid: {invalid_count}")

    if invalid_count > 0:
        write_quarantine(invalid_df, "FactStream", "stream_id")
        print(f"  ✓ Quarantined {invalid_count} rows")

    if valid_count == 0:
        print("  ✓ No valid data")
        return

    deduped_df = valid_df.withColumn(
        "rn", F.row_number().over(
            Window.partitionBy("stream_id").orderBy(F.col("stream_timestamp").desc())
        )
    ).filter(F.col("rn") == 1).drop("rn")

    with_audit = deduped_df \
        .withColumn("silver_ingest_timestamp", F.current_timestamp()) \
        .withColumn("bronze_ingest_timestamp", F.col("ingest_timestamp")) \
        .withColumn("input_file_path",         F.col("captured_file_path"))
    
    with_audit=with_audit.drop("dq_fail_reason")

    with_audit.createOrReplaceTempView("int_silver_factstream")

    spark.sql("""
        MERGE INTO spotify_catalog.silver.factstream t
        USING int_silver_factstream s
        ON t.stream_id = s.stream_id
        WHEN MATCHED THEN UPDATE SET
            t.date_key                = s.date_key,
            t.listen_duration         = s.listen_duration,
            t.device_type             = s.device_type
        WHEN NOT MATCHED THEN INSERT (
            stream_id, user_id, track_id, date_key, listen_duration,
            device_type, stream_timestamp, dq_is_valid,
            silver_ingest_timestamp, bronze_ingest_timestamp, input_file_path
        ) VALUES (
            s.stream_id, s.user_id, s.track_id, s.date_key, s.listen_duration,
            s.device_type, s.stream_timestamp, s.dq_is_valid,
            s.silver_ingest_timestamp, s.bronze_ingest_timestamp, s.input_file_path
        )
    """)

    print(f"✅ [FactStream SCD1] Batch {batch_id} complete\n")


# ═══════════════════════════════════════════════════════════════════════════
# START STREAMS
# ═══════════════════════════════════════════════════════════════════════════
dimuser_stream = (
    spark.readStream
         .table("spotify_catalog.bronze.dimuser")
         .writeStream
         .foreachBatch(pipeline_dqm_dedup_scd2_for_dimuser)
         .option("checkpointLocation",
                 f"{SILVER_BASE}/checkpoints/DimUser")
         .trigger(availableNow=True)
         .start()
)
dimuser_stream.awaitTermination()
factstream_stream = (
    spark.readStream
         .table("spotify_catalog.bronze.factstream")
         .writeStream
         .foreachBatch(pipeline_dqm_dedup_scd1_for_factstream)
         .option("checkpointLocation",
                 f"{SILVER_BASE}/checkpoints/FactStream")
         .trigger(availableNow=True)
         .start()
)
# Wait for both
factstream_stream.awaitTermination()

print("Silver layer completed")