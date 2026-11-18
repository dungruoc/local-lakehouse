"""Create the `lakehouse` bucket, write one object and list it back."""
 
from common import s3_create_bucket
import sys
 
client = s3_create_bucket(sys.argv[1])
 