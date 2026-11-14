"""Bronze: the landing files, as they arrived."""
 
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
 
spark = SparkSession.active()
 
# Unity Catalog manages these tables: it picks their location under the
# catalog's storage root. These properties mark a Delta table as catalog-managed.
MANAGED = {
    "delta.feature.catalogManaged": "supported",
    "delta.checkpoint.writeStatsAsJson": "true",
    "delta.checkpoint.writeStatsAsStruct": "true",
}
 
 
@dp.materialized_view(format="delta", table_properties=MANAGED)
def orders_bronze():
    return spark.read.parquet("s3://lakehouse/landing/orders/")
 
 
@dp.materialized_view(format="delta", table_properties=MANAGED)
def locations():
    return spark.read.parquet("s3://lakehouse/landing/locations/")
