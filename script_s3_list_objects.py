from common import s3_list_object
import sys

contents = s3_list_object(sys.argv[1], "")
print(contents)
if "Contents" in contents:
    for obj in contents["Contents"]:
        print(obj)