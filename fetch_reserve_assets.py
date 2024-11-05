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


url = f"{baseurl}/{api_key}/subgraphs/id/{subraph_id}"
headers = {
    "Content-Type": "application/json",
}


query = """{{
  reserves(orderBy: underlyingAsset, where: {{underlyingAsset_gt: "{preceding_address}"}}) {{
    underlyingAsset
    symbol
    name
  }}
}}"""


reserve_assets = []
preceding_address = "0x"

while True:
    payload = {
        "query": query.format(preceding_address=preceding_address),
        "operationName": "Subgraphs",
        "variables": {},
    }

    response = requests.post(url, json=payload, headers=headers)
    reserve_assets_batch = [reserve for reserve in response.json()["data"]["reserves"]]
    if not reserve_assets_batch:
        break
    reserve_assets += reserve_assets_batch
    logger.debug(
        {"reserve_assets": len(reserve_assets), "preceding_address": preceding_address}
    )
    preceding_address = reserve_assets_batch[-1]["underlyingAsset"]


save_data(reserve_assets, "reserve_assets")

preceding_address.upper()

reserve_assets
