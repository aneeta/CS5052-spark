import requests
from geojson_rewind import rewind
import json

# LA_GEOJSON = rewind(requests.get(
#     "https://files.planning.data.gov.uk/dataset/local-authority-district.geojson"
#     # "https://martinjc.github.io/UK-GeoJSON/json/eng/topo_lad.json"
# ).json(), False)

with open("data/local-authority-district.json") as f:
    LA_GEOJSON = rewind(json.load(f), False)


with open("data/region.json") as f:
    REGION_GEOJSON = rewind(json.load(f), False)
