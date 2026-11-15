from common import spark
import sys

session = spark()

session.sql(f"Describe {sys.argv[1]}").show()

# session.sql(f"describe DETAIL {sys.argv[1]}").show(truncate=False)

session.sql(f"DESCRIBE EXTENDED {sys.argv[1]}").show(truncate=False)
# print(session.sql(f"select * from {sys.argv[1]}").schema.json())
