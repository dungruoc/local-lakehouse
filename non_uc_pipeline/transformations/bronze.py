"""Bronze: the landing files, as they arrived."""
 
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.active()
 

 
@dp.materialized_view (
    format="delta",
    name="orders_bronze",
    table_properties={
        "location": "s3://lakehouse/tables/bronze/orders",
    })
def orders_bronze():
    return spark.read.parquet("s3://lakehouse/landing/orders/").withColumn(
        "event_date", f.to_date("ts")
    )


@dp.materialized_view(
    format="delta",
    name="locations",
    table_properties={
        "location": "s3://lakehouse/tables/bronze/locations",
    })
def locations():
    return spark.read.parquet("s3://lakehouse/landing/locations/")
