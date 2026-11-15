
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.active()

@dp.materialized_view (
    format="delta",
    name="revenue_by_brand",
    table_properties={
        "location": "s3://lakehouse/tables/gold/revenue_by_brand",
    })
def revenue_by_brand():
    df = spark.read.format("delta").load("s3://lakehouse/tables/silver/orders")
    return spark.sql(
       """
        SELECT
          brand,
          count(*) AS orders,
          round(sum(total), 2) AS revenue,
          round(avg(total), 2) AS avg_order
        FROM {my_dataframe}
        WHERE event_type = 'order_created'
        GROUP BY brand
       """,
       my_dataframe=df
    )