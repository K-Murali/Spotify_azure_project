from pyspark.sql import functions as F

BASE_PATH  = "/Volumes/spotify_catalog/datalake/rawdata"
CHKPT_BASE = "abfss://deltalake@mkazureproject1.dfs.core.windows.net/bronze"

# ── helpers ───────────────────────────────────────────────────────────────────
def add_bronze_audit_cols(df):
    return df \
        .withColumn("input_file_path",  F.col("_metadata.file_path")) \
        .withColumn("ingest_timestamp", F.current_timestamp()) \
        .withColumn("ingest_date",      F.current_date())

def read_autoloader(table_name):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",         "parquet")
            .option("cloudFiles.schemaLocation",  f"{CHKPT_BASE}/{table_name}/schema")
            .option("schemaEvolutionMode",        "addNewColumns")
            .option("cloudFiles.inferColumnTypes","true")
            .load(f"{BASE_PATH}/{table_name}")
    )

def write_bronze(df, table_name):
    return (
        df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{CHKPT_BASE}/{table_name}/checkpoints")
            .option("mergeSchema",      "true")
            .trigger(availableNow=True)
            .option("path", "abfss://deltalake@mkazureproject1.dfs.core.windows.net/bronze/FactStream/data")
            .table(f"spotify_catalog.bronze.{table_name}")
    )

# ── read → audit → write ──────────────────────────────────────────────────────
tables = ["FactStream", "DimUser", "DimArtist", "DimTrack", "DimDate"]

queries = [
    write_bronze(add_bronze_audit_cols(read_autoloader(t)), t)
    for t in tables
]

for q in queries:
    q.awaitTermination()