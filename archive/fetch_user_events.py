import logging
import os

import requests
from dotenv import load_dotenv

from utils import save_data

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
query = """{
  users(orderBy: id, orderDirection: desc) {
    supplyHistory {
      action
      amount
      assetPriceUSD
      txHash
      reserve {
        symbol
        underlyingAsset
        decimals
      }
      timestamp
    }
    id
  }
}"""


payload = {
    "query": query,
    "operationName": "Subgraphs",
    "variables": {},
}
response = requests.post(url, json=payload, headers=headers)
users_events = response.json()["data"]["users"]

save_data(users_events, "users_events")

user_event = users_events[0]

supply_history = user_event["supplyHistory"]
supply_action = supply_history[0]
supply_action["amount"]
