from common import uc
import sys

for sc in uc("GET", f"/schemas?catalog_name={sys.argv[1]}")["schemas"]:
    print(sc)