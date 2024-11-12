import logging
from datetime import datetime, timezone

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

subgraph_helper = SubgraphHelper()

query = """
{
  atokenBalanceHistoryItems(
    orderBy: timestamp
    orderDirection: asc
    where: {timestamp_gte: $timestamp_gte}
  ) {
    scaledATokenBalance
    currentATokenBalance
    timestamp
    index
    id
    userReserve {
      user {
        id
      }
      reserve {
        symbol
        underlyingAsset
      }
    }
  }
}"""
query_parsed = subgraph_helper.format_query(query, "timestamp_gte")

first_timestamp = datetime_to_posix(date_str_to_datetime(configs["first_date"]))
atoken_balance_histories = []
while True:
    payload = {
        "query": query_parsed.format(timestamp_gte=first_timestamp),
        "operationName": "Subgraphs",
        "variables": {},
    }
    response = requests.post(
        subgraph_helper.url, json=payload, headers=subgraph_helper.headers
    )
    atoken_balance_histories_batch = response.json()["data"][
        "atokenBalanceHistoryItems"
    ]
    if not atoken_balance_histories_batch:
        break
    atoken_balance_histories += atoken_balance_histories_batch
    print(
        datetime.fromtimestamp(first_timestamp, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        f"- {len(atoken_balance_histories)}",
    )
    first_timestamp = atoken_balance_histories_batch[-1]["timestamp"] + 1

save_data(atoken_balance_histories, "atoken_balance_histories", data_layer="raw")