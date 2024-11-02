# TODO 0 reserves
# - Set User E Mode
# - Approve Delegation
# - No data at all
# - ? https://etherscan.io/address/0xfff30c67eea809123596252e132d30e1eb75bc83

# DONE 0 aTokenBalanceHistory?
#   Transactions before september
# TODO `currentATokenBalance` calculation?

import json
import logging
import os

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


baseurl = "https://gateway.thegraph.com/api"
api_key = os.environ["THEGRAPH_API_KEY"]
subraph_id = "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"

# 2024-09-01
start_timestamp = 1725148800

url = f"{baseurl}/{api_key}/subgraphs/id/{subraph_id}"
headers = {
    "Content-Type": "application/json",
}
query = """{{
  users(orderBy: id, orderDirection: desc) {{
    id
    reserves {{
      currentATokenBalance
      aTokenBalanceHistory(where: {{timestamp_gte: {start_timestamp}}}) {{
        currentATokenBalance
        index
        scaledATokenBalance
        timestamp
      }}
    }}
  }}
}}"""


# user_ids = []
# last_id = "0"

# while True:
#     payload = {
#         "query": query.format(start_timestamp=start_timestamp, last_id=last_id),
#         "operationName": "Subgraphs",
#         "variables": {},
#     }

#     response = requests.post(url, json=payload, headers=headers)
#     user_ids_batch = [user["id"] for user in response.json()["data"]["users"]]
#     if not user_ids_batch:
#         break
#     user_ids += user_ids_batch
#     logger.debug({"user_ids": len(user_ids), "last_id": last_id})
#     last_id = user_ids_batch[-1]


def save_data(data_to_save: dict | list, filename: str):
    os.makedirs("data", exist_ok=True)

    with open(f"data/{filename}.json", "w") as f:
        json.dump(data_to_save, f)


payload = {
    "query": query.format(start_timestamp=start_timestamp),
    "operationName": "Subgraphs",
    "variables": {},
}
response = requests.post(url, json=payload, headers=headers)
user_atokens = response.json()["data"]["users"]

assert all([len(user["reserves"]) <= 100 for user in user_atokens])
assert all(
    [
        len(reserve["aTokenBalanceHistory"]) <= 100
        for user in user_atokens
        for reserve in user["reserves"]
    ]
)


save_data(user_atokens, "user_atokens")
