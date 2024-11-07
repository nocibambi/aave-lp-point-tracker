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

from utils import datetime_to_posix, save_data

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


baseurl = "https://gateway.thegraph.com/api"

subraph_id = "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"


url = f"{baseurl}/{os.environ['THEGRAPH_API_KEY']}/subgraphs/id/{subraph_id}"
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
    Web3.HTTPProvider(f"https://mainnet.infura.io/v3/{os.environ['INFURA_API_KEY']}")
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
pool_contract = w3.eth.contract(address=address, abi=contract_abi)

# WETH reserve
token_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
atoken_address = "0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8"


# TODO get abi of aToken implementation
# at https://etherscan.io/address/0x7effd7b47bfd17e52fb7559d3f924201b9dbff3d#code

url = "https://api.etherscan.io/api"
params = {
    "module": "contract",
    "action": "getsourcecode",
    "address": atoken_address,
    "apikey": os.environ["ETHERSCAN_API_KEY"],
}
etherscan_atoken_respone = requests.get(url, params=params)
atoken_implementation_address = etherscan_atoken_respone.json()["result"][0][
    "Implementation"
]
# def get_asset_api(contract_address: str, api_key: str) -> list | dict:

url = "https://api.etherscan.io/api"
params = {
    "module": "contract",
    "action": "getabi",
    "address": atoken_implementation_address,
    "apikey": os.environ["ETHERSCAN_API_KEY"],
}
implementation_abi = requests.get(url, params=params).json()["result"]


json.loads(implementation_abi)


asset_contract = w3.eth.contract(address=atoken_address, abi=implementation_abi)

balance_on_etherscan = asset_contract.functions.balanceOf(
    w3.to_checksum_address("0xffb827fd3fd24103cc1a8b1db2f968369e75c00d")
).call()
balance_on_etherscan / 10**18

# =============


normalized_income = pool_contract.functions.getReserveNormalizedIncome(
    token_address
).call()

print(normalized_income)

#

ray_decimals = "1" + "0" * 27
token_decimals = "1" + "0" * user_reserve["reserve"]["decimals"]

datetime.fromtimestamp(1702271519)
user_reserve
# As on etherscan
# e.g. https://etherscan.io/token/0x4d5f47fa6a74757f35c14fd3a6ef8e3c9bc514e8?a=0xffb827fd3fd24103cc1a8b1db2f968369e75c00d
actual_balance = (
    Decimal(user_reserve["scaledATokenBalance"]) / Decimal(token_decimals)
) * (Decimal(str(normalized_income)) / Decimal(ray_decimals))
actual_balance
