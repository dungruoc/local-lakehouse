"""Check both streaming queries against what the generator sent."""
 
from common import spark
from pyspark.sql import functions as f
 
EVENT = "event_id STRING, event_type STRING, ts STRING, location_id INT, order_id STRING, body STRING"
 
session = spark()
 
 
def topic(name):
    """Everything in a Kafka topic so far, read once as a batch."""
    return (
        session.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", name)
        .option("startingOffsets", "earliest")
        .load()
    )
 
 
sent = (
    topic("orders")
    .select(f.from_json(f.col("value").cast("string"), EVENT).alias("e"))
    .where("e.event_type = 'order_created' AND e.location_id IS NOT NULL")
    .agg(f.count("*").alias("records"), f.count_distinct("e.event_id").alias("distinct"))
    .first()
)
totals = session.table("lakehouse.tutorial.live_totals").orderBy("location_id")
totals.show(truncate=False)
 
print(f"order_created records sent:        {sent['records']:,}")
print(f"distinct order_created event ids:  {sent['distinct']:,}")
print(f"orders in live_totals:             {totals.agg(f.sum('orders')).first()[0]:,}")
 
# How long each record spent in the Real-Time Mode query, Kafka to Kafka.
spent = topic("orders-clean").select(
    (f.unix_millis("timestamp")
     - f.get_json_object(f.col("value").cast("string"), "$.received_ms").cast("long")).alias("ms")
)
p50, p99 = spent.agg(f.percentile_approx("ms", [0.5, 0.99])).first()[0]
print(f"\norders-clean: {spent.count():,} records, p50 {p50} ms, p99 {p99} ms in Spark")