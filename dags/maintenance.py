"""Every night, compact the external Delta tables and clean up files nothing needs."""
 
from datetime import datetime
 
from airflow.sdk import DAG, task
 
TABLES = ["lakehouse.tutorial.orders_raw", "lakehouse.tutorial.live_totals"]
 
with DAG("delta_maintenance", schedule="0 3 * * *", start_date=datetime(2026, 9, 1), catchup=False):
 
    @task
    def optimize_and_vacuum():
        from pyspark.sql import SparkSession
 
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
        for table in TABLES:
            before = spark.sql(f"DESCRIBE DETAIL {table}").first()["numFiles"]
            spark.sql(f"OPTIMIZE {table}").collect()  # many small files into a few big ones
            spark.sql(f"VACUUM {table} RETAIN 168 HOURS").collect()  # keep a week of history
            after = spark.sql(f"DESCRIBE DETAIL {table}").first()["numFiles"]
            print(f"{table}: {before} files -> {after}")
 
    optimize_and_vacuum()