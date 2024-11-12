import logging

import requests
from dotenv import load_dotenv

from aave_point_tracker.utils.subgraph_helpers import SubgraphHelper
from aave_point_tracker.utils.utils import (
    date_str_to_datetime,
    datetime_to_posix,
    load_configs,
    save_data,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

load_dotenv()
configs = load_configs()

subgraph = SubgraphHelper()

query = """
{
  users(orderBy: id, orderDirection: asc, where: {id_gt: "$id_gt"}) {
    id
    reserves {
      aTokenBalanceHistory(
        first: 1
        orderBy: timestamp
        orderDirection: desc
        where: {timestamp_lte: $timestamp_lte}
      ) {
        id
        scaledATokenBalance
        timestamp
        currentATokenBalance
        index
      }
      reserve {
        underlyingAsset
        symbol
      }
    }
  }
}"""
query_parsed = subgraph.format_query(query, "id_gt", "timestamp_lte")

last_date_posix = datetime_to_posix(date_str_to_datetime(configs["first_date"]))
starting_balances = []
last_id = "0"
while True:
    payload = {
        "query": query_parsed.format(id_gt=last_id, timestamp_lte=last_date_posix),
        "operationName": "Subgraphs",
        "variables": {},
    }
    response = requests.post(subgraph.url, json=payload, headers=subgraph.headers)
    starting_balances_batch = response.json()["data"]["users"]
    if not starting_balances_batch:
        break
    starting_balances += starting_balances_batch
    last_id = starting_balances_batch[-1]["id"]
logger.debug(f"starting_balances: {len(starting_balances)}")
save_data(starting_balances, "starting_balances", data_layer="raw")
