import dlt

expectations={
    "rule_1":"stream_id IS NOT NULL",
}

@dlt.table
def Factstream_stg():
    df=spark.readStream.table("spotify_catalog.bronze.factstream")
    return df


dlt.create_streaming_table(
    name="factstream",
    expect_all_or_drop=expectations
    )

dlt.create_auto_cdc_flow(
    target='factstream',
    source='factstream_stg',
    keys=['stream_id'],
    sequence_by="stream_timestamp",
    stored_as_scd_type=1,
    track_history_except_column_list=None,
    name=None,
    once=False
)