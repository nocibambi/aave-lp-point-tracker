import logging
from collections import defaultdict
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

subgraph_helper = SubgraphHelper()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

load_dotenv()
configs = load_configs()

query = """{
  reserveParamsHistoryItems(
    where: {timestamp_gt: $timestamp_gt, timestamp_lte: $timestamp_lte}
    orderBy: timestamp
    orderDirection: asc
  ) {
    liquidityIndex
    timestamp
    reserve {
      symbol
      underlyingAsset
    }
  }
}"""
first_date_posix = datetime_to_posix(
    date_str_to_datetime(configs["first_date"]), buffer="early"
)
last_date_posix = datetime_to_posix(
    date_str_to_datetime(configs["last_date"]) + timedelta(days=1), buffer="late"
)
query_parsed = query_parsed = subgraph_helper.format_query(
    query, "timestamp_gt", "timestamp_lte"
).format(
    timestamp_gt=first_date_posix,
    timestamp_lte=last_date_posix,
)
logger.debug(query_parsed)
reserve_liquidity_index_histories: defaultdict = defaultdict(list)
while True:
    payload = {
        "query": subgraph_helper.format_query(
            query, "timestamp_gt", "timestamp_lte"
        ).format(
            timestamp_gt=first_date_posix,
            timestamp_lte=last_date_posix,
        ),
        "operationName": "Subgraphs",
        "variables": {},
    }
    response = requests.post(
        subgraph_helper.url, json=payload, headers=subgraph_helper.headers
    )
    index_history_batch = response.json()["data"]["reserveParamsHistoryItems"]
    for index in index_history_batch:
        reserve_liquidity_index_histories[index["reserve"]["underlyingAsset"]].append(
            {
                "timestamp": index["timestamp"],
                "liquidityIndex": index["liquidityIndex"],
            }
        )
    if len(index_history_batch) < 100:
        break
    first_date_posix = index_history_batch[-1]["timestamp"]

logger.debug(
    {
        reserve: len(indexes)
        for reserve, indexes in reserve_liquidity_index_histories.items()
    }
)
save_data(
    reserve_liquidity_index_histories,
    "reserve_liquidity_index_histories",
    data_layer="raw",
)
