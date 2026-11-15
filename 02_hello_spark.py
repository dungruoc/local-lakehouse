"""Send one query to the cluster through Spark Connect."""
 
from common import SPARK_REMOTE, spark
 
session = spark()
print(f"Connected to Spark {session.version} at {SPARK_REMOTE}")
session.range(1_000_000).selectExpr("sum(id) AS total").show()