import logging
import os
from datetime import timedelta

import requests
from dotenv import load_dotenv

from utils import save_data, get_configs, date_str_to_datetime, datetime_to_posix

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

configs = get_configs()


first_date_posix = datetime_to_posix(
    date_str_to_datetime(configs["first_date"]), buffer="early"
)
last_date_posix = datetime_to_posix(
    date_str_to_datetime(configs["last_date"]) + timedelta(days=1), buffer="late"
)

first_date_posix, last_date_posix

baseurl = configs["thegraph"]["baseurl"]
subraph_id = configs["thegraph"]["subgraph_id"]
api_key = os.environ["THEGRAPH_API_KEY"]

url = f"{baseurl}/{api_key}/subgraphs/id/{subraph_id}"
headers = {
    "Content-Type": "application/json",
}
query = """{
  reserves(orderBy: underlyingAsset) {
    underlyingAsset
    symbol
    name
    aToken {
      id
    }
  }
}"""


payload = {
    "query": query,
    "operationName": "Subgraphs",
    "variables": {},
}

response = requests.post(url, json=payload, headers=headers)
reserve_assets = [reserve for reserve in response.json()["data"]["reserves"]]
save_data(reserve_assets, "reserve_assets")
assert len(reserve_assets) <= 100, "Response might have more than 100 records..."
