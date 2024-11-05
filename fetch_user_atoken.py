# TODO 0 reserves
# - Set User E Mode
# - Approve Delegation
# - No data at all
# - ? https://etherscan.io/address/0xfff30c67eea809123596252e132d30e1eb75bc83

# DONE 0 aTokenBalanceHistory?
#   Transactions before september
# TODO `currentATokenBalance` calculation?

import logging
import os

import requests
from dotenv import load_dotenv

from utils import date_str_to_posix, save_data

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


baseurl = "https://gateway.thegraph.com/api"
api_key = os.environ["THEGRAPH_API_KEY"]
subraph_id = "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"


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


payload = {
    "query": query.format(start_timestamp=date_str_to_posix("2024-09-01")),
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

{user["id"]: len(user["reserves"]) for user in user_atokens}


[
    user
    for user in user_atokens
    if user["id"] == "0xffab14b181409170378471b13ff2bff5be012c64"
]
