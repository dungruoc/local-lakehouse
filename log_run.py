"""Log an MLflow run from a PySpark job, then read it back from the tracking server."""
 
import mlflow
from common import MLFLOW_TRACKING_URI, s3, spark
from pyspark.sql import functions as f
 
gold = spark().table("lakehouse.medallion.revenue_by_brand")
 
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("ghost-kitchen")
with mlflow.start_run(run_name="brand-revenue") as run:
    stats = gold.agg(
        f.count("*").alias("brands"),
        f.round(f.sum("revenue"), 2).alias("revenue"),
        f.max("avg_order").alias("best_avg_order"),
    ).first()
    mlflow.log_param("source_table", "lakehouse.medallion.revenue_by_brand")
    mlflow.log_metrics(stats.asDict())
    mlflow.log_text(gold.toPandas().to_csv(index=False), "revenue_by_brand.csv")
 
logged = mlflow.get_run(run.info.run_id)
print(f"{logged.info.run_name}: {logged.info.status}")
for name, value in sorted(logged.data.metrics.items()):
    print(f"  {name} = {value}")
 
# The artifact went through the tracking server into the bucket.
for obj in s3().list_objects_v2(Bucket="lakehouse", Prefix="mlflow/")["Contents"]:
    print(obj["Key"])