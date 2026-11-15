from common import spark
import sys

session = spark()

df = session.read.format("delta").load(sys.argv[1])
session.sql("Describe {table}", table=df).show()

# session.sql(f"describe DETAIL {sys.argv[1]}").show(truncate=False)

session.sql("DESCRIBE EXTENDED {table}", table=df).show(truncate=False)
# print(session.sql(f"select * from {sys.argv[1]}").schema.json())
