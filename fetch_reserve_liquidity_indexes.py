import logging
import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import defaultdict

from utils import (
    save_data,
    load_configs,
    datetime_to_posix,
    date_str_to_datetime,
)

from subgraph_utils import SubgraphHelper

subgraph_helper = SubgraphHelper()

logger = logging.getLogger(__name__)

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
query_parsed = subgraph_helper.format_query(query, "timestamp_gt", "timestamp_lte")

first_date_posix = datetime_to_posix(
    date_str_to_datetime(configs["first_date"]), buffer="early"
)
last_date_posix = datetime_to_posix(
    date_str_to_datetime(configs["last_date"]) + timedelta(days=1), buffer="late"
)

reserve_liquidity_indexes: defaultdict = defaultdict(list)
while True:
    query_configured = query_parsed.format(
        timestamp_gt=first_date_posix,
        timestamp_lte=last_date_posix,
    )
    payload = {
        "query": query_configured,
        "operationName": "Subgraphs",
        "variables": {},
    }
    response = requests.post(
        subgraph_helper.url, json=payload, headers=subgraph_helper.headers
    )
    index_history_batch = response.json()["data"]["reserveParamsHistoryItems"]
    for index in index_history_batch:
        reserve_liquidity_indexes[index["reserve"]["symbol"]].append(
            {
                "timestamp": index["timestamp"],
                "liquidityIndex": index["liquidityIndex"],
            }
        )
    if len(index_history_batch) < 100:
        break
    logger.info(
        first_date_posix,
        last_date_posix,
        len(index_history_batch),
        sum(len(values) for values in reserve_liquidity_indexes.values()),
    )
    first_date_posix = index_history_batch[-1]["timestamp"]

logger.info(
    {reserve: len(indexes) for reserve, indexes in reserve_liquidity_indexes.items()}
)
save_data(reserve_liquidity_indexes, "reserve_liquidity_indexes")
