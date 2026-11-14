"""Run the medallion pipeline every day, on the cluster, through Spark Connect."""
 
from datetime import datetime
 
from airflow.providers.apache.spark.operators.spark_pipelines import SparkPipelinesOperator
from airflow.sdk import DAG, task
 
with DAG("medallion", schedule="@daily", start_date=datetime(2026, 9, 1), catchup=False):
 
    @task
    def prepare():
        """Create the schema, or drop the last run's tables (SDP won't replace them)."""
        from pyspark.sql import SparkSession
 
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
        spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.medallion")
        for row in spark.sql("SHOW TABLES IN lakehouse.medallion").collect():
            spark.sql(f"DROP TABLE lakehouse.medallion.{row.tableName}")
 
    run_pipeline = SparkPipelinesOperator(
        task_id="run_pipeline",
        pipeline_spec="/opt/airflow/pipeline/spark-pipeline.yml",
        pipeline_command="run",
        conn_id="spark_connect_default",
    )
 
    @task
    def check_gold():
        from pyspark.sql import SparkSession
 
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
        brands = spark.table("lakehouse.medallion.revenue_by_brand").count()
        print(f"revenue_by_brand has {brands} rows")
        if brands == 0:
            raise ValueError("the gold table is empty")
 
    prepare() >> run_pipeline >> check_gold()