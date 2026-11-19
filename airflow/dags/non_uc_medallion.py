"""Run the medallion pipeline every day, on the cluster, through Spark Connect."""
 
from datetime import datetime
 
from airflow.sdk import DAG, task
from pyspark.sql import functions as f


BRONZE_ORDERS_PATH = "s3://lakehouse/tables/bronze/orders"
BRONZE_LOCATIONS_PATH = "s3://lakehouse/tables/bronze/new_locations"
SILVER_ORDERS_PATH = "s3://lakehouse/tables/silver/orders"
GOLD_REVENUE_BY_BRAND_PATH = "s3://lakehouse/tables/gold/revenue_by_brand"

TABLES = [BRONZE_ORDERS_PATH, BRONZE_LOCATIONS_PATH, SILVER_ORDERS_PATH, GOLD_REVENUE_BY_BRAND_PATH]

with DAG("non_uc_medallion", schedule="@daily", start_date=datetime(2026, 9, 1), catchup=False):
 

    @task
    def bronze_orders():
        from pyspark.sql import SparkSession 
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()

        bronze_df = spark.read.parquet("s3://lakehouse/landing/orders/").withColumn(
            "event_date", f.to_date("ts")
        )
        bronze_df.write.format("delta").mode("overwrite").save(BRONZE_ORDERS_PATH)


    @task
    def bronze_locations():
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()

        bronze_df = spark.read.parquet("s3://lakehouse/landing/locations/")
        bronze_df.write.format("delta").mode("overwrite").save(BRONZE_LOCATIONS_PATH)


    @task
    def silver_orders():
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()

        orders_df = spark.read.format("delta").load(BRONZE_ORDERS_PATH)
        locations_df = spark.read.format("delta").load(BRONZE_LOCATIONS_PATH)
        silver_df =  spark.sql("""
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


        silver_df.write.format("delta").mode("overwrite").save(SILVER_ORDERS_PATH)


    @task
    def gold_revenue_by_brand():
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()

        silver_df = spark.read.format("delta").load(SILVER_ORDERS_PATH)
        gold_df = spark.sql(
        """
            SELECT
            brand,
            count(*) AS orders,
            round(sum(total), 2) AS revenue,
            round(avg(total), 2) AS avg_order
            FROM {silver_df}
            WHERE event_type = 'order_created'
            GROUP BY brand
        """,
        silver_df=silver_df
        )

        gold_df.write.format("delta").mode("overwrite").save(GOLD_REVENUE_BY_BRAND_PATH)

    @task
    def check_gold():
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()

        brands = spark.read.format("delta").load(GOLD_REVENUE_BY_BRAND_PATH).count()
        print(f"revenue_by_brand has {brands} rows")
        if brands == 0:
            raise ValueError("the gold table is empty")


    @task
    def optimize():
        """Create the schema, or drop the last run's tables (SDP won't replace them)."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()

        for table_path in TABLES:
            spark.sql(f"OPTIMIZE delta.`{table_path}`").collect()
            spark.sql(f"VACUUM delta.`{table_path}` RETAIN 6 HOURS").collect()
 
    [ bronze_orders(), bronze_locations() ] >> silver_orders() >> gold_revenue_by_brand() >> check_gold() >> optimize()

