import logging
import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import defaultdict

from utils import (
    save_data,
    get_configs,
    datetime_to_posix,
    parse_query,
    date_str_to_datetime,
)

logger = logging.getLogger(__name__)

load_dotenv()
configs = get_configs()

baseurl = configs["thegraph"]["baseurl"]
subraph_id = configs["thegraph"]["subgraph_id"]
api_key = os.environ["THEGRAPH_API_KEY"]

url = f"{baseurl}/{api_key}/subgraphs/id/{subraph_id}"
headers = {"Content-Type": "application/json"}

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

query_parsed = parse_query(query, "id_gt", "timestamp_lte")
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
    response = requests.post(url, json=payload, headers=headers)
    users_starting_balances_batch = response.json()["data"]["users"]
    if not users_starting_balances_batch:
        break
    users_starting_balances += users_starting_balances_batch
    print({"users_starting_balances": len(users_starting_balances), "last_id": last_id})
    last_id = users_starting_balances_batch[-1]["id"]

save_data(users_starting_balances, "users_starting_balances")
