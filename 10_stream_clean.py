"""Real-Time Mode: clean each order event from Kafka into another topic as it arrives."""
 
from common import spark
from pyspark.sql import functions as f
 
EVENT = "event_id STRING, event_type STRING, ts STRING, location_id INT, order_id STRING, body STRING"
 
session = spark()
orders = (
    session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")  # Kafka as the cluster sees it
    .option("subscribe", "orders")
    .load()
    .select(f.from_json(f.col("value").cast("string"), EVENT).alias("e"), "timestamp")
)
 
clean = orders.where("e.event_type = 'order_created' AND e.location_id IS NOT NULL").select(
    f.col("e.order_id").alias("key"),
    f.to_json(f.struct(
        "e.event_id",
        "e.location_id",
        f.get_json_object("e.body", "$.brand").alias("brand"),
        f.get_json_object("e.body", "$.total").cast("double").alias("total"),
        f.unix_millis("timestamp").alias("received_ms"),  # when it reached Kafka
    )).alias("value"),
)
 
query = (
    clean.writeStream.queryName("orders_clean")
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "orders-clean")
    .option("checkpointLocation", "s3://checkpoints/orders_clean")
    # .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/orders_clean")
    .outputMode("update")  # the output mode Real-Time Mode supports
    .trigger(realTime="5 minutes")  # how often to checkpoint, not a latency target
    .start()
)
print("orders -> orders-clean, in Real-Time Mode. Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()