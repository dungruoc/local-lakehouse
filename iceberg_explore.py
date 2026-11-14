from common import UC_URL
from pyiceberg.catalog import load_catalog


catalog = load_catalog(
    "retail",
    type="rest",
    uri="http://localhost:8081/api/2.1/unity-catalog/iceberg",
    warehouse="lakehouse",
    token="not-used",
)

print(catalog.list_namespaces())