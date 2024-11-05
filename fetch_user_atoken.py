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
from datetime import datetime
from decimal import Decimal

import requests
from dotenv import load_dotenv
from web3 import Web3, eth
from web3.contract import Contract

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
query = """{
  users(orderBy: id, orderDirection: desc) {
    id
    reserves(where: {scaledATokenBalance_gt: "0"}) {
      aTokenBalanceHistory {
        currentATokenBalance
        scaledATokenBalance
        timestamp
        index
      }
      currentATokenBalance
      lastUpdateTimestamp
      reserve {
        underlyingAsset
        symbol
        name
        decimals
        liquidityIndex
      }
      scaledATokenBalance
    }
  }
}"""


payload = {
    "query": query,  # .format(start_timestamp=date_str_to_posix("2024-09-01")),
    "operationName": "Subgraphs",
    "variables": {},
}
response = requests.post(url, json=payload, headers=headers)
users_atokens = response.json()["data"]["users"]

assert all([len(user["reserves"]) <= 100 for user in users_atokens])
assert all(
    [
        len(reserve["aTokenBalanceHistory"]) <= 100
        for user in users_atokens
        for reserve in user["reserves"]
    ]
)


save_data(users_atokens, "users_atokens")

{user["id"]: len(user["reserves"]) for user in users_atokens}


user_atokens = [
    user
    for user in users_atokens
    if user["id"] == "0xffb827fd3fd24103cc1a8b1db2f968369e75c00d"
][0]
user_atokens

user_reserve = user_atokens["reserves"][-1]
user_reserve
# =============

# Initialize a web3 instance
w3 = Web3(
    Web3.HTTPProvider("https://mainnet.infura.io/v3/f5b918fb4a9d4ba18bab6b1a4813a659")
)
w3.is_connected()
# Ethereum Aave V3 Pool
address = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
contract_abi = [
    {
        "inputs": [{"name": "asset", "type": "address"}],
        "name": "getReserveNormalizedIncome",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]
contract = w3.eth.contract(address=address, abi=contract_abi)

# WETH reserve
asset_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
normalized_income = contract.functions.getReserveNormalizedIncome(asset_address).call()

print(normalized_income)

# =============

ray_decimals = "1" + "0" * 27
token_decimals = "1" + "0" * user_reserve["reserve"]["decimals"]

datetime.fromtimestamp(1702271519)
user_reserve
# As on etherscan
# e.g. https://etherscan.io/token/0x4d5f47fa6a74757f35c14fd3a6ef8e3c9bc514e8?a=0xffb827fd3fd24103cc1a8b1db2f968369e75c00d
actual_balance = (
    Decimal(user_reserve["scaledATokenBalance"]) / Decimal(token_decimals)
) * (Decimal(str(normalized_income)) / Decimal(ray_decimals))

actual_value = 0.05295483

1034140776328126615277731400
1034140968364316492083327087
