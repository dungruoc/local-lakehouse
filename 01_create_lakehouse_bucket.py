"""Create the `lakehouse` bucket, write one object and list it back."""
 
from common import create_bucket
 
client = create_bucket("lakehouse")
 
client.put_object(Bucket="lakehouse", Key="hello.txt", Body=b"hello, lakehouse\n")
for obj in client.list_objects_v2(Bucket="lakehouse")["Contents"]:
    print(obj["Key"], obj["Size"], "bytes")