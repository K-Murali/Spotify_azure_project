from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CreateSilverTablesSQL").getOrCreate()

SILVER_BASE = "abfss://deltalake@mkazureproject1.dfs.core.windows.net/silver"

spark.sql("USE CATALOG spotify_catalog")
spark.sql("USE SCHEMA silver")


# ═══════════════════════════════════════════════════════════════════════════
# DROP EXISTING TABLES
# ═══════════════════════════════════════════════════════════════════════════

print("Dropping existing tables...")
spark.sql("DROP TABLE IF EXISTS spotify_catalog.silver.dimuser")
print("✓ Dropped dimuser")

spark.sql("DROP TABLE IF EXISTS spotify_catalog.silver.factstream")
print("✓ Dropped factstream")

spark.sql("DROP TABLE IF EXISTS spotify_catalog.silver.dqm_log")
print("✓ Dropped dqm_log")


# ═══════════════════════════════════════════════════════════════════════════
# DimUser — SCD2 | surrogate key + open/close cols
# ═══════════════════════════════════════════════════════════════════════════
spark.sql(f"""
    CREATE TABLE spotify_catalog.silver.dimuser (
        user_sk                 LONG      NOT NULL,
        user_id                 LONG,
        user_name               STRING,
        country                 STRING,
        subscription_type       STRING,
        scd_start_date          DATE,
        scd_end_date            DATE,
        is_current              BOOLEAN,
        updated_at              TIMESTAMP,
        dq_is_valid             BOOLEAN,
        silver_ingest_timestamp TIMESTAMP,
        bronze_ingest_timestamp TIMESTAMP,
        input_file_path         STRING
    )
    USING DELTA
    LOCATION '{SILVER_BASE}/DimUser/data'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite'   = 'true',
        'delta.autoOptimize.autoCompact'     = 'true',
        'delta.enableChangeDataFeed'         = 'true',
        'delta.logRetentionDuration'         = 'interval 60 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")
print("✓ DimUser created")


# ═══════════════════════════════════════════════════════════════════════════
# FactStream — SCD1 | deduped, PARTITIONED by date_key
# ═══════════════════════════════════════════════════════════════════════════
spark.sql(f"""
    CREATE TABLE spotify_catalog.silver.factstream (
        stream_id               LONG,
        user_id                 LONG,
        track_id                LONG,
        date_key                INT,
        listen_duration         INT,
        device_type             STRING,
        stream_timestamp        TIMESTAMP,
        dq_is_valid             BOOLEAN,
        silver_ingest_timestamp TIMESTAMP,
        bronze_ingest_timestamp TIMESTAMP,
        input_file_path         STRING
    )
    USING DELTA
    PARTITIONED BY (date_key)
    LOCATION '{SILVER_BASE}/FactStream/data'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite'   = 'true',
        'delta.autoOptimize.autoCompact'     = 'true',
        'delta.enableChangeDataFeed'         = 'true',
        'delta.logRetentionDuration'         = 'interval 60 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")
print("✓ FactStream created")


# ═══════════════════════════════════════════════════════════════════════════
# dqm_log — bad rows from any table land here
# ═══════════════════════════════════════════════════════════════════════════
spark.sql(f"""
    CREATE TABLE spotify_catalog.silver.dqm_log (
        source_table         STRING,
        natural_key          STRING,
        raw_record           STRING,
        dq_fail_reason       STRING,
        quarantine_timestamp TIMESTAMP,
        input_file_path      STRING
    )
    USING DELTA
    LOCATION '{SILVER_BASE}/dqm_log/data'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite'   = 'true',
        'delta.autoOptimize.autoCompact'     = 'true',
        'delta.enableChangeDataFeed'         = 'true',
        'delta.logRetentionDuration'         = 'interval 60 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")
print("✓ dqm_log created")

print("\n✅ All Silver layer tables created successfully!")