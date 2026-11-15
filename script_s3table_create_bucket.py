from common import s3tables
import sys

client = s3tables()

response = client.create_table_bucket(
    name=sys.argv[1]
)

print(response)