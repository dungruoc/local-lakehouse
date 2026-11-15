"""Create the `lakehouse` bucket, write one object and list it back."""
 
from common import s3tables_list_buckets
import sys
 
buckets, _ = s3tables_list_buckets()
for b in buckets:
    print(b)
 