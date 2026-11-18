"""Micro-batch: deduplicate orders and keep running totals per location, in Delta."""
 
from common import spark
from pyspark.sql import functions as f
 
EVENT = "event_id STRING, event_type STRING, ts STRING, location_id INT, order_id STRING, body STRING"
 
session = spark()
session.conf.set("spark.sql.shuffle.partitions", "4")
session.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.tutorial.live_totals (
        location_id INT, orders BIGINT, revenue DOUBLE, last_order_at TIMESTAMP
    ) USING delta LOCATION 's3://lakehouse/tables/live_totals'
""")
 
totals = (
    session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders")
    .load()
    .select(f.from_json(f.col("value").cast("string"), EVENT).alias("e"))
    .select("e.*", f.to_timestamp("e.ts").alias("event_time"))
    .where("event_type = 'order_created' AND location_id IS NOT NULL")
    .withWatermark("event_time", "10 minutes")  # how long to remember an event id
    .dropDuplicates(["event_id", "event_time"])
    .groupBy("location_id")
    .agg(
        f.count("*").alias("orders"),
        f.round(f.sum(f.get_json_object("body", "$.total").cast("double")), 2).alias("revenue"),
        f.max("event_time").alias("last_order_at"),
    )
)
 
query = (
    totals.writeStream.queryName("live_totals")
    .format("delta")
    .outputMode("complete")  # each batch rewrites the four rows with the current totals
    .option("checkpointLocation", "s3://checkpoints/live_totals")
    # .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/live_totals")
    .trigger(processingTime="1 second")
    .toTable("lakehouse.tutorial.live_totals")
)
print("orders -> lakehouse.tutorial.live_totals, every second. Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()