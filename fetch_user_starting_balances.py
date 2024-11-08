import logging
import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import defaultdict

from subgraph_utils import SubgraphHelper
from utils import (
    save_data,
    load_configs,
    datetime_to_posix,
    date_str_to_datetime,
)

logger = logging.getLogger(__name__)

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
    }
  }
}"""

query_parsed = subgraph.format_query(query, "id_gt", "timestamp_lte")
last_date_posix = datetime_to_posix(date_str_to_datetime(configs["first_date"]))

# print(query_parsed.format(id_gt=last_id, timestamp_lte=last_date_posix))

users_starting_balances = []
last_id = "0"
while True:
    payload = {
        "query": query_parsed.format(id_gt=last_id, timestamp_lte=last_date_posix),
        "operationName": "Subgraphs",
        "variables": {},
    }
    response = requests.post(subgraph.url, json=payload, headers=subgraph.headers)
    users_starting_balances_batch = response.json()["data"]["users"]
    if not users_starting_balances_batch:
        break
    users_starting_balances += users_starting_balances_batch
    print({"users_starting_balances": len(users_starting_balances), "last_id": last_id})
    last_id = users_starting_balances_batch[-1]["id"]

save_data(users_starting_balances, "users_starting_balances")
