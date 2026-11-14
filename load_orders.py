"""Write the week's events as a Delta table in two commits, then time-travel."""
 
from common import s3, spark
from pyspark.sql import functions as f
 
TABLE_PATH = "s3://lakehouse/tables/orders_raw"
 
session = spark()
events = session.read.parquet("s3://lakehouse/landing/orders/").withColumn(
    "event_date", f.to_date("ts")
)
 
# Commit 0: the first day. Commit 1: the other six.
events.where("event_date = '2026-09-01'").write.format("delta").save(TABLE_PATH)
events.where("event_date > '2026-09-01'").write.format("delta").mode("append").save(TABLE_PATH)
 
session.sql(f"DESCRIBE HISTORY delta.`{TABLE_PATH}`").select(
    "version", "operation", "operationMetrics.numOutputRows"
).show()
 
now = session.read.format("delta").load(TABLE_PATH).count()
then = session.read.format("delta").option("versionAsOf", 0).load(TABLE_PATH).count()
print(f"rows now: {now:,}   rows at version 0: {then:,}\n")
 
# Underneath, it's files in the bucket: Parquet data plus the _delta_log.
for obj in s3().list_objects_v2(Bucket="lakehouse", Prefix="tables/orders_raw/")["Contents"]:
    print(obj["Key"])