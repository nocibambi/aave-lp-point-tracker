import logging
import os
import json
import requests
from dotenv import load_dotenv

from utils import save_data, get_configs, date_str_to_posix, parse_query

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

configs = get_configs()
first_date_posix = date_str_to_posix(configs["first_date"], buffer="early")
last_date_posix = date_str_to_posix(configs["last_date"], buffer="late")

first_date_posix, last_date_posix

baseurl = configs["thegraph"]["baseurl"]
subraph_id = configs["thegraph"]["subgraph_id"]
api_key = os.environ["THEGRAPH_API_KEY"]

aave_pool_address_provider = configs["aave"]["ethereum_v3_main"][
    "pool_address_provider"
]

with open(f"{os.environ['DATA_PATH']}/reserve_assets.json") as f:
    reserves = json.load(f)

url = f"{baseurl}/{api_key}/subgraphs/id/{subraph_id}"
headers = {
    "Content-Type": "application/json",
}

query = """{
  reserve(id: "$id") {
    underlyingAsset
    symbol
    paramsHistory(where: {timestamp_gt: $timestamp_gt, timestamp_lt: $timestamp_lt}) {
      liquidityIndex
      timestamp
    }
  }
}"""

query_parsed = parse_query(query, "id", "timestamp_gt", "timestamp_lt")
print(query_parsed)

reserve_liquidity_indexes: dict = {}
for reserve in reserves:
    reserve_asset = reserve["underlyingAsset"]
    reserve_liquidity_indexes[reserve_asset] = []

    while True:
        payload = {
            "query": query_parsed.format(
                id=reserve_asset + aave_pool_address_provider,
                timestamp_gt=first_date_posix,
                timestamp_lt=last_date_posix,
            ),
            "operationName": "Subgraphs",
            "variables": {},
        }

        response = requests.post(url, json=payload, headers=headers)
        index_history_batch = response.json()["data"]["reserve"]["paramsHistory"]
        reserve_liquidity_indexes[reserve_asset] += index_history_batch
        if len(index_history_batch) < 100:
            break
        first_date_posix = index_history_batch[-1]["timestamp"]

    logger.debug(
        f"Fetched liquidity indexes for {reserve['symbol']}:\n"
        + str(len(reserve_liquidity_indexes[reserve_asset]))
    )

save_data(reserve_liquidity_indexes, "reserve_liquidity_indexes")

logger.debug(
    {
        reserve["symbol"]: len(reserve_liquidity_indexes[reserve["underlyingAsset"]])
        for reserve in reserves
    }
)
