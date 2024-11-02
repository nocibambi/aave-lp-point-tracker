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

url = f"{baseurl}/{api_key}/subgraphs/id/{subraph_id}"
headers = {
    "Content-Type": "application/json",
}
query = """query MyQuery {{
  users(
    first: 100
    where: {{id_gt: "{last_id}"}}
  ) {{
    id
  }}
}}"""

user_ids = []
last_id = "0"

while True:
    payload = {
        "query": query.format(last_id=last_id),
        "operationName": "Subgraphs",
        "variables": {},
    }

    response = requests.post(url, json=payload, headers=headers)
    user_ids_batch = [user["id"] for user in response.json()["data"]["users"]]
    if not user_ids_batch:
        break
    user_ids += user_ids_batch
    logger.debug({"user_ids": len(user_ids), "last_id": last_id})
    last_id = user_ids_batch[-1]


os.makedirs("data", exist_ok=True)

with open("data/user_ids.json", "w") as f:
    json.dump(user_ids, f)
