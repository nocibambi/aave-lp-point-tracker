import logging
from datetime import timedelta

import requests
from dotenv import load_dotenv

from aave_point_tracker.utils.subgraph_helpers import SubgraphHelper
from aave_point_tracker.utils.utils import (
    date_str_to_datetime,
    datetime_to_posix,
    load_configs,
    save_data,
)

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

configs = load_configs()

subgraph_helper = SubgraphHelper()

first_date_posix = datetime_to_posix(
    date_str_to_datetime(configs["first_date"]), buffer="early"
)
last_date_posix = datetime_to_posix(
    date_str_to_datetime(configs["last_date"]) + timedelta(days=1), buffer="late"
)

query = """{
  reserves(orderBy: underlyingAsset) {
    underlyingAsset
    symbol
    name
    decimals
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

response = requests.post(
    subgraph_helper.url, json=payload, headers=subgraph_helper.headers
)
reserve_assets = [reserve for reserve in response.json()["data"]["reserves"]]
assert len(reserve_assets) <= 100, "Response might have more than 100 records..."
save_data(reserve_assets, "reserve_assets", data_layer="raw")
