"""Bronze: the landing files, as they arrived."""
 
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.active()

@dp.materialized_view (
    format="delta",
    name="orders_silver",
    table_properties={
        "location": "s3://lakehouse/tables/silver/orders",
    })
def orders_silver():
    orders_df = spark.read.format("delta").load("s3://lakehouse/tables/bronze/orders")
    locations_df = spark.read.format("delta").load("s3://lakehouse/tables/bronze/locations")
    return spark.sql("""
      SELECT DISTINCT
        o.event_id,
        o.event_type,
        to_timestamp(o.ts) AS event_time,
        o.order_id,
        l.city,
        get_json_object(o.body, '$.brand') AS brand,
        CAST(get_json_object(o.body, '$.total') AS DOUBLE) AS total
      FROM {orders_bronze} o
      JOIN {locations} l ON o.location_id = l.location_id
      """,
      orders_bronze=orders_df, locations=locations_df)
