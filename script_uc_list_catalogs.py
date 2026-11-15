from common import uc

for c in uc("GET", "/catalogs")["catalogs"]:
    print(c)