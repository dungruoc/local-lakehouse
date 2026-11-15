"""Create the `lakehouse` bucket, write one object and list it back."""
 
from common import create_bucket
import sys
 
client = create_bucket(sys.argv[1])
 