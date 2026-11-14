import io
import sys
from common import s3
import pandas as pd


client = s3()
bucket_name = sys.argv[1]
file_key = sys.argv[2]

response = client.get_object(Bucket=bucket_name, Key=file_key)

# Option A: Pass StreamingBody directly
df = pd.read_csv(response["Body"])

# Option B: Decode explicitly (useful if you need custom encoding handling)
# csv_content = response["Body"].read().decode("utf-8")
# df = pd.read_csv(io.StringIO(csv_content))

print(df.head())