from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ViewTest").getOrCreate()

mv_name = "spotify_catalog.view.v_dimuser"

spark.sql(f"""
    CREATE OR REPLACE VIEW {mv_name} AS
    SELECT * FROM spotify_catalog.silver.dimuser
    WHERE is_current = true
""")
print("✓ View Created")