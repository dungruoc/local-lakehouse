"""Create the `lakehouse` bucket, write one object and list it back."""
 
from common import s3_list_bucket
import sys
 
buckets, _ = s3_list_bucket()
for b in buckets:
    print(b)
 